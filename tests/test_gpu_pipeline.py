from contextlib import closing

import numpy as np
import pytest
import torch

from ml.models import FEATURES, NUMERIC_DIM, make_model, seed_all
from ml.pipeline import prefetch
from ml.shards import ShardDataset, ShardWriter, training_groups
from ml.training import (backward_group, optimizer_update, preflight, save_checkpoint,
                         score, train_batches, trusted_load)


DEVICES = ['cpu', pytest.param('cuda', marks=pytest.mark.skipif(
    not torch.cuda.is_available(), reason='CUDA unavailable'))]


@pytest.fixture
def synthetic_train(tmp_path):
    rng = np.random.default_rng(272)
    writer = ShardWriter(tmp_path/'TRAIN', np.zeros(NUMERIC_DIM), np.ones(NUMERIC_DIM), capacity=13)
    for i in range(31):
        length = 128 if i == 10 else 1 + (i * 3) % 7
        category = np.zeros(128, dtype=np.uint16)
        behavior = np.zeros(128, dtype=np.uint8)
        hour = np.zeros(128, dtype=np.uint8)
        gap = np.zeros(128, dtype=np.float32)
        category[:length] = rng.integers(1, 16385, size=length)
        behavior[:length] = rng.integers(1, 5, size=length)
        hour[:length] = rng.integers(1, 25, size=length)
        gap[1:length] = rng.random(length - 1)
        writer.add(dict(user_id=str(i + 1), as_of=1512057600 + i % 2, length=length,
                        category=category, behavior=behavior, hour=hour, gap=gap,
                        numeric=rng.normal(size=NUMERIC_DIM), label=i % 2, history_event_count=length,
                        cohort='SEEN_TRAIN' if i % 3 else 'UNSEEN_TRAIN'))
    writer.finish()
    return ShardDataset(tmp_path/'TRAIN')


@pytest.mark.parametrize('bucket', [False, True])
def test_prefetch_preserves_order_and_partial_groups(synthetic_train, bucket):
    def groups(seed=42, epoch=2):
        return training_groups(synthetic_train, 4, 9, seed=seed, epoch=epoch,
                               bucket_by_length=bucket, trim_sequences=True)

    def keys(source):
        return [key for batches, _, _ in source for batch in batches for key in batch['sample_key']]

    synchronous = list(groups())
    with closing(prefetch(groups())) as asynchronous:
        for (left, weights, rows), (right, other_weights, other_rows) in zip(synchronous, asynchronous, strict=True):
            assert (weights, rows) == (other_weights, other_rows)
            for a, b in zip(left, right, strict=True):
                for key in a:
                    np.testing.assert_array_equal(a[key], b[key])
    assert [n for _, _, n in synchronous] == [9, 9, 9, 4]
    observed = keys(synchronous)
    assert len(observed) == len(set(observed)) == synthetic_train.manifest['rows']
    assert set(observed) == {key for batch in synthetic_train.batches(shuffle=False) for key in batch['sample_key']}
    assert observed == keys(groups())
    assert observed != keys(groups(seed=43))
    assert observed != keys(groups(epoch=3))


@pytest.mark.parametrize('algorithm', ['LOGISTIC', 'GRU'])
@pytest.mark.parametrize('device_name', DEVICES)
def test_same_seed_has_bitwise_identical_weights_and_scores(synthetic_train, tmp_path, algorithm, device_name):
    device = torch.device(device_name)

    def run(repeat):
        seed_all(43)
        model = make_model(algorithm).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=.001, weight_decay=1e-4)
        scaler = torch.amp.GradScaler('cuda', enabled=device.type == 'cuda')
        updates = 0
        for epoch in (1, 2):
            batches = train_batches(synthetic_train, device, algorithm, 4, 8, 43, epoch,
                                    bucket_by_length=algorithm == 'GRU', prefetch_batches=True)
            with closing(batches):
                for group, (weights, _) in batches:
                    optimizer.zero_grad(set_to_none=True)
                    backward_group(model, scaler, group, weights, device)
                    updates += not optimizer_update(model, optimizer, scaler)
        assert updates > 0
        output = tmp_path/f'scores-{repeat}.npy'
        score(synthetic_train, model, device, algorithm, output, batch_size=5, prefetch_batches=True)
        return model.state_dict(), output.read_bytes()

    left, left_scores = run(0)
    right, right_scores = run(1)
    for key in left:
        assert torch.equal(left[key], right[key]), key
    assert left_scores == right_scores


@pytest.mark.parametrize('algorithm', ['LOGISTIC', 'GRU'])
@pytest.mark.parametrize('device_name', DEVICES)
def test_optimized_fp32_score_matches_legacy_checkpoint(synthetic_train, tmp_path, algorithm, device_name):
    device = torch.device(device_name)
    seed_all()
    checkpoint = tmp_path/'checkpoint.pt'
    digest = save_checkpoint(checkpoint, make_model(algorithm).state_dict())
    model = make_model(algorithm).to(device).eval()
    model.load_state_dict(trusted_load(checkpoint, digest, tmp_path))
    features = FEATURES if algorithm == 'GRU' else ('numeric',)
    metadata = ('label', 'cohort', 'sample_key', 'as_of')
    actual_path = tmp_path/'actual.npy'
    with torch.autocast(device.type):
        result = score(synthetic_train, model, device, algorithm, actual_path, prefetch_batches=True)
    actual = np.load(actual_path, allow_pickle=False)
    probabilities = []
    legacy = list(synthetic_train.batches(4, shuffle=False, columns=(*features, *metadata)))
    with torch.no_grad():
        for batch in legacy:
            inputs = {key: torch.from_numpy(batch[key]).to(device=device, dtype=(
                torch.long if key in ('behavior', 'category', 'hour', 'length') else torch.float32))
                      for key in features}
            probabilities.append(torch.sigmoid(model(inputs).float()).cpu().numpy().astype(np.float64))
    np.testing.assert_allclose(actual['score'], np.concatenate(probabilities), atol=1e-5, rtol=0)
    for key in metadata:
        np.testing.assert_array_equal(actual[key], np.concatenate([batch[key] for batch in legacy]))
    assert result['sample_order_sha256'] == synthetic_train.manifest['sample_order_sha256']


@pytest.mark.skipif(not torch.cuda.is_available(), reason='CUDA unavailable')
def test_gru_cuda_preflight_observes_amp_and_finite_update(synthetic_train):
    _, evidence = preflight(synthetic_train, 'GRU', torch.device('cuda'))
    accepted = evidence[-1]
    assert accepted['amp'] and accepted['logits_dtype'] == 'torch.float16'
    assert accepted['gru_dtype'] == 'torch.float16'
    assert accepted['optimizer_updates'] > 0
