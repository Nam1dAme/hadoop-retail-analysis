"""Pure-numpy GBDT baseline: capacity, early stopping, determinism and TRAIN-only binning."""
import numpy as np
import pytest

from ml.gbdt import (DEFAULT_PARAMS, bin_counts, compute_bin_edges, fit, load_model,
                     predict_proba, sample_train_rows, save, sigmoid)
from ml.experiment import ALGORITHM_ORDER, select
from ml.metrics import metrics
from ml.models import NUMERIC_DIM
from orchestration.common import ManifestError, encode, sha


def _linear_auc(x, y, directory):
    """IRLS logistic fit: the linear upper bound the tree must beat on XOR."""
    design = np.column_stack([np.ones(len(x)), x.astype(np.float64)])
    beta = np.zeros(design.shape[1])
    for _ in range(60):
        p = sigmoid(design @ beta)
        weights = np.maximum(p * (1. - p), 1e-8)
        hessian = design.T @ (design * weights[:, None])
        gradient = design.T @ (p - y.astype(np.float64))
        beta += np.linalg.solve(hessian + 1e-4 * np.eye(len(beta)), -gradient)
    return metrics(sigmoid(design @ beta), y, directory=directory)['roc_auc']


def test_gbdt_learns_nonlinear_interaction_beyond_logistic(tmp_path):
    rng = np.random.default_rng(7)
    data = rng.normal(size=(24_000, NUMERIC_DIM)).astype(np.float32)
    labels = ((data[:, 0] > 0) == (data[:, 1] > 0)).astype(np.int8)  # XOR
    train = (data[:20_000], labels[:20_000])
    validation = (data[20_000:], labels[20_000:])
    model = fit(train, validation, seed=42, max_trees=200, patience=15,
                min_child_samples=50, sample_limit=20_000)
    probability = predict_proba(model, validation[0])
    tree_auc = metrics(probability, validation[1], directory=tmp_path)['roc_auc']
    linear_auc = _linear_auc(train[0], train[1], tmp_path)
    assert tree_auc > .9, (tree_auc, linear_auc)
    assert linear_auc < .7, (tree_auc, linear_auc)
    assert np.isfinite(probability).all() and ((probability >= 0) & (probability <= 1)).all()
    assert all(max(1, len(edges) - 1) <= DEFAULT_PARAMS['max_bins'] for edges in model['bin_edges'])


def test_validation_logloss_early_stops_before_max_trees():
    rng = np.random.default_rng(3)
    train = (rng.normal(size=(1_200, NUMERIC_DIM)).astype(np.float32),
             rng.integers(0, 2, 1_200).astype(np.int8))
    validation = (rng.normal(size=(1_200, NUMERIC_DIM)).astype(np.float32),
                  rng.integers(0, 2, 1_200).astype(np.int8))
    model = fit(train, validation, seed=1, max_trees=60, patience=2,
                min_child_samples=10, max_depth=3, sample_limit=1_200)
    assert len(model['history']) < 60
    assert model['best_iteration'] < len(model['history'])
    assert len(model['trees']) == model['best_iteration']
    assert model['history'][-1]['validation_logloss'] >= model['validation_logloss'] - 1e-9


def test_json_checkpoint_roundtrip_and_seed_determinism(tmp_path):
    rng = np.random.default_rng(11)
    data = rng.normal(size=(3_000, NUMERIC_DIM)).astype(np.float32)
    labels = (data[:, 2] + data[:, 3] > 0).astype(np.int8)
    train = (data[:2_400], labels[:2_400])
    validation = (data[2_400:], labels[2_400:])
    params = dict(seed=5, max_trees=12, patience=4, min_child_samples=20, sample_limit=2_400)
    first = fit(train, validation, **params)
    second = fit(train, validation, **params)
    assert encode(first) == encode(second)
    expected = predict_proba(first, validation[0])
    assert np.array_equal(expected, predict_proba(second, validation[0]))

    checkpoint = tmp_path/'model.json'
    digest = save(first, checkpoint)
    assert digest == sha(checkpoint)
    restored = load_model(checkpoint, digest, tmp_path)
    assert np.array_equal(expected, predict_proba(restored, validation[0]))
    with pytest.raises(ManifestError):
        load_model(checkpoint, '0'*64, tmp_path)


def test_binary_feature_remains_splittable(tmp_path):
    rng = np.random.default_rng(31)
    rows = 6_000
    data = rng.normal(size=(rows, NUMERIC_DIM)).astype(np.float32)
    data[:, 0] = rng.integers(0, 2, rows)  # TRAIN has exactly two distinct values
    labels = data[:, 0].astype(np.int8)
    model = fit((data[:4_000], labels[:4_000]), (data[4_000:], labels[4_000:]),
                seed=1, max_trees=5, patience=2, min_child_samples=20, sample_limit=4_000)
    assert bin_counts(model['bin_edges'])[0] == 2
    probability = predict_proba(model, data[4_000:])
    assert metrics(probability, labels[4_000:], directory=tmp_path)['roc_auc'] > .95


def test_shard_sample_labels_stay_aligned():
    rng = np.random.default_rng(37)
    features = rng.normal(size=(1_000, NUMERIC_DIM)).astype(np.float32)
    labels = (features[:, 0] > 0).astype(np.int8)

    class Shard:
        manifest = {'rows': len(features)}

        def batches(self, batch_size=512, shuffle=False, columns=None):
            for start in range(0, len(features), 37):
                values = dict(numeric=features[start:start + 37], label=labels[start:start + 37])
                yield {name: values[name] for name in columns}

    sample, sampled_labels = sample_train_rows(Shard(), limit=200, seed=5)
    assert len(sample) == len(sampled_labels) == 200
    assert np.array_equal(sampled_labels, (sample[:, 0] > 0).astype(np.int8))


def test_bin_edges_are_train_only():
    rng = np.random.default_rng(19)
    train_x = rng.normal(size=(2_000, NUMERIC_DIM)).astype(np.float32)
    train_y = (train_x[:, 0] > 0).astype(np.int8)
    sample, _ = sample_train_rows((train_x, train_y), limit=2_000, seed=13)
    expected = compute_bin_edges(sample)
    models = []
    for shift in (-8., 8.):
        validation = (rng.normal(loc=shift, size=(500, NUMERIC_DIM)).astype(np.float32),
                      rng.integers(0, 2, 500).astype(np.int8))
        models.append(fit((train_x, train_y), validation, seed=13, sample_limit=2_000,
                          max_trees=3, patience=1, min_child_samples=20))
    assert models[0]['bin_edges'] == expected
    assert models[1]['bin_edges'] == expected
    assert bin_counts(models[0]['bin_edges']) == models[1]['bin_counts']


def test_score_writes_gbdt_probability_file(tmp_path):
    import hashlib
    import torch

    from ml.metrics import SCORE_DTYPE
    from ml.training import score

    rng = np.random.default_rng(23)
    data = rng.normal(size=(300, NUMERIC_DIM)).astype(np.float32)
    labels = (data[:, 0] > 0).astype(np.int8)
    model = fit((data, labels), (data, labels), seed=2, max_trees=4, patience=2,
                min_child_samples=10, sample_limit=300)
    keys = np.array([f'{i:03d}'.encode() for i in range(len(data))], dtype='S64')
    cohort = np.arange(len(data), dtype=np.uint8) % 2
    as_of = np.full(len(data), 1511539200, dtype=np.int64)
    digest = hashlib.sha256()
    for key in keys:
        digest.update(bytes(key) + b'\n')

    class SmallDataset:
        manifest = dict(rows=len(data), sample_order_sha256=digest.hexdigest())

        def batches(self, batch_size=512, shuffle=False, columns=None, **kwargs):
            values = dict(numeric=data, label=labels, cohort=cohort, sample_key=keys, as_of=as_of)
            yield {key: values[key] for key in columns}

    output = tmp_path/'validation.npy'
    result = score(SmallDataset(), None, torch.device('cpu'), 'GBDT', output, gbdt=model)
    saved = np.load(output, allow_pickle=False)
    assert saved.dtype == SCORE_DTYPE
    assert np.array_equal(saved['score'], predict_proba(model, data))
    assert np.array_equal(saved['sample_key'], keys)
    assert np.array_equal(saved['cohort'], cohort)
    assert result['sample_order_sha256'] == digest.hexdigest()


def test_gbdt_joins_selection_tie_break_order():
    assert ALGORITHM_ORDER['GBDT'] > ALGORITHM_ORDER['RULE']

    def row(algorithm):
        return dict(algorithm=algorithm, validation_metrics={'ALL': {'ap': .5, 'logloss': .4}}, parameter_count=10)

    assert select([row('RULE'), row('GBDT')])['algorithm'] == 'RULE'
    assert select([row('GBDT'), dict(row('GBDT'), parameter_count=5)])['parameter_count'] == 5
