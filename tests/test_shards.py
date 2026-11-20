import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pytest

from ml.models import NUMERIC_DIM
from ml.shards import ShardDataset, ShardWriter
from orchestration.common import ManifestError, atomic_json, sha


MEAN = np.arange(NUMERIC_DIM, dtype=np.float64)
STD = np.full(NUMERIC_DIM, 2., dtype=np.float64)


def sample(i, label=0, cohort='SEEN_TRAIN', numeric=None):
    """Minimal valid row in the E01 shard schema."""
    return dict(
        sample_key=hashlib.sha256((f'{i:02d}' + '|1512057600').encode()).hexdigest(),
        user_id=f'{i:02d}', as_of=1512057600, length=1,
        category=[7] + [0] * 127, behavior=[1] + [0] * 127, hour=[1] + [0] * 127,
        gap=[0.] * 128, numeric=list(range(NUMERIC_DIM)) if numeric is None else numeric,
        label=label, history_event_count=1, cohort=cohort,
    )


def write_shard(path, rows, capacity=8, mean=MEAN, std=STD):
    writer = ShardWriter(path, mean, std, capacity=capacity)
    for row in rows:
        writer.add(row)
    return writer.finish()


def test_writer_dataset_roundtrip_columns_dtype_shape(tmp_path):
    path = tmp_path / 'TRAIN'
    rows = [sample(i, i % 2, 'SEEN_TRAIN' if i % 3 else 'UNSEEN_TRAIN') for i in range(7)]
    manifest = write_shard(path, rows, capacity=3)
    assert [shard['rows'] for shard in manifest['shards']] == [3, 3, 1]

    dataset = ShardDataset(path, verify=True)
    batches = list(dataset.batches(batch_size=2, shuffle=False))
    assert sum(len(batch['label']) for batch in batches) == len(rows)

    expected_dtype = dict(as_of='<i8', length='u1', category='<u2', behavior='u1',
                          hour='u1', gap='<f4', numeric='<f4', label='i1',
                          history_event_count='<i8', cohort='u1', sample_key='S64')
    expected_shape = dict(category=(128,), behavior=(128,), hour=(128,),
                          gap=(128,), numeric=(NUMERIC_DIM,))
    for batch in batches:
        assert set(batch) == set(expected_dtype)
        for key, dtype in expected_dtype.items():
            assert batch[key].dtype == np.dtype(dtype), key
            assert batch[key].shape == (len(batch['label']), *expected_shape.get(key, ())), key

    actual = {key: np.concatenate([batch[key] for batch in batches])
              for key in expected_dtype}
    order = [row['sample_key'].encode() for row in rows]
    assert actual['sample_key'].tolist() == order
    for i, key in enumerate(order):
        assert actual['category'][i, 0] == 7 and not actual['category'][i, 1:].any()
        assert actual['behavior'][i, 0] == 1 and not actual['behavior'][i, 1:].any()
        assert actual['hour'][i, 0] == 1 and not actual['hour'][i, 1:].any()
        assert not actual['gap'][i].any()
        assert actual['as_of'][i] == 1512057600
        assert actual['length'][i] == 1
        assert actual['history_event_count'][i] == 1
        assert actual['label'][i] == rows[i]['label']
        assert actual['cohort'][i] == (1 if rows[i]['cohort'] == 'SEEN_TRAIN' else 0)
    np.testing.assert_allclose(actual['numeric'],
                               ((np.asarray([row['numeric'] for row in rows]) - MEAN) / STD).astype(np.float32),
                               rtol=0, atol=1e-6)


def test_dataset_rejects_legacy_v1_cache(tmp_path):
    path = tmp_path / 'TRAIN'
    write_shard(path, [sample(i, i % 2) for i in range(4)])
    manifest = json.loads((path / 'manifest.json').read_text())
    manifest['schema_version'] = '1.0'
    atomic_json(path / 'manifest.json', manifest)
    with pytest.raises(ManifestError, match='Legacy 14-dim sample cache'):
        ShardDataset(path, verify=False)


def test_verify_rejects_tampered_shard_file(tmp_path):
    path = tmp_path / 'TRAIN'
    write_shard(path, [sample(i, i % 2) for i in range(4)], capacity=3)
    shard = path / 'shard-00000'
    feature = shard / 'behavior.npy'
    original = np.load(feature)
    modified = original.copy()
    modified[0, 0] = 2
    np.save(feature, modified)

    # verify=False still loads the changed bytes, so the assertion below is about
    # the integrity check itself rather than an unrelated read failure.
    batch = next(ShardDataset(path, verify=False).batches(shuffle=False))
    assert batch['behavior'][0, 0] == 2
    with pytest.raises(ManifestError):
        ShardDataset(path, verify=True)


def test_normalization_uses_supplied_train_statistics(tmp_path):
    train_rows = [sample(i, i % 2, numeric=[value + i for value in range(NUMERIC_DIM)])
                  for i in range(4)]
    # Deliberately different population: own statistics would produce different values.
    validation_rows = [sample(10 + i, i % 2, numeric=[value * 10 + i for value in range(NUMERIC_DIM)])
                       for i in range(3)]

    train = np.asarray([row['numeric'] for row in train_rows], dtype=np.float64)
    validation = np.asarray([row['numeric'] for row in validation_rows], dtype=np.float64)
    mean, std = train.mean(axis=0), train.std(axis=0)
    train_path, validation_path = tmp_path / 'TRAIN', tmp_path / 'VALIDATION'
    write_shard(train_path, train_rows, mean=mean, std=std)
    write_shard(validation_path, validation_rows, mean=mean, std=std)

    train_values = np.concatenate([batch['numeric'] for batch in
                                   ShardDataset(train_path).batches(shuffle=False)])
    validation_values = np.concatenate([batch['numeric'] for batch in
                                        ShardDataset(validation_path).batches(shuffle=False)])
    np.testing.assert_allclose(train_values, ((train - mean) / std).astype(np.float32),
                               rtol=0, atol=1e-6)
    np.testing.assert_allclose(validation_values, ((validation - mean) / std).astype(np.float32),
                               rtol=0, atol=1e-6)
    assert not np.allclose(validation_values,
                           (validation - validation.mean(axis=0)) / validation.std(axis=0),
                           atol=1e-6)


def test_prepare_applies_train_normalization_to_every_split(tmp_path, monkeypatch):
    """Python consumes the TRAIN-only J06 moments and applies them to all splits."""
    import ml.shards as shards
    import orchestration.audit as audit
    import orchestration.samples as samples

    splits = ('TRAIN', 'VALIDATION', 'TEST', 'FORECAST')
    rows = []
    for split in splits:
        for i in range(2):
            row = sample(i, i % 2, cohort='SEEN_TRAIN' if i else 'UNSEEN_TRAIN')
            row['split'] = split
            rows.append(row)
    normal = dict(count=2, mean=[float(i) for i in range(NUMERIC_DIM)], std=[1.] * NUMERIC_DIM)
    stage = lambda path: {'outputs': [{'path': path, 'key_class': 'text'}]}
    stages = {
        'J05': dict(stage('/raw'), counters={'SAMPLES': {s: 2 for s in splits}}),
        'J06': stage('/moments'), 'J04users': stage('/users'),
        'J01': dict(stage('/clean'), counters={'DAILY': {}}),
    }
    writers = []

    class Storage:
        normalization = normal

        def dump(self, paths):
            path = paths[0]['path']
            if path == '/moments':
                return ['moments\t' + json.dumps(self.normalization)]
            return [row['sample_key'] + '\t' + json.dumps(row) for row in rows] if path == '/raw' else []

        def put(self, *args):
            pass

    class FakeWriter:
        def __init__(self, path, mean, std):
            self.path, self.mean, self.std, self.rows = path, mean, std, []
            writers.append(self)

        def add(self, row):
            self.rows.append(row)

        def finish(self):
            return dict(rows=len(self.rows),
                        positive_count=sum(row['label'] == 1 for row in self.rows),
                        negative_count=sum(row['label'] == 0 for row in self.rows),
                        seen_count=sum(row['cohort'] == 'SEEN_TRAIN' for row in self.rows),
                        unseen_count=sum(row['cohort'] == 'UNSEEN_TRAIN' for row in self.rows),
                        sample_order_sha256='0' * 64)

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, *args):
            pass

        def fetchone(self):
            return {'experiment_id': 'experiment-1'}

    lease = SimpleNamespace(config={'paths': {'work_dir': str(tmp_path / 'work')},
                                    'synthetic': True},
                            db=SimpleNamespace(cursor=Cursor))
    expected = {s: dict(rows=2, positive_count=1, negative_count=1, seen=1, unseen=1)
                for s in splits}
    monkeypatch.setattr(samples, 'Hdfs', lambda config, owner: Storage())
    monkeypatch.setattr(samples, 'get_stage', lambda owner, batch, stage: stages[stage])
    monkeypatch.setattr(audit, 'populations', lambda storage, users: {'samples': expected})
    monkeypatch.setattr(shards, 'ShardDataset', lambda *args, **kwargs: None)
    monkeypatch.setattr(samples, 'ShardWriter', FakeWriter)
    def run_stage(owner, batch, stage, inputs, config, export):
        manifest = export(Storage(), '/remote', tmp_path / 'local', 'run-1')
        return dict(manifest, outputs=[], run_id='run-1')

    monkeypatch.setattr(samples, 'stage_run', run_stage)

    result = samples.prepare(lease, 'batch')
    assert {writer.path.name for writer in writers} == set(splits)
    assert all(writer.mean == normal['mean'] and writer.std == normal['std'] for writer in writers)
    assert result['samples']['normalization'] == normal
    cache_manifest = json.loads((tmp_path / 'work/sample-cache/run-1/manifest.json').read_text())
    assert cache_manifest['schema_version'] == '2.0'
    assert cache_manifest['split_version'] == 'daily-48h-24h-v2'

    Storage.normalization = dict(normal, count=3)
    with pytest.raises(ManifestError, match='Normalization population differs'):
        samples.prepare(lease, 'batch')
    Storage.normalization = dict(normal, std=[0.] + [1.] * (NUMERIC_DIM - 1))
    with pytest.raises(ManifestError, match='Invalid normalization'):
        samples.prepare(lease, 'batch')


@pytest.mark.parametrize('layout', ['<u2', '<i4'])
def test_category_layout_compatibility(tmp_path, layout):
    path = tmp_path / 'TRAIN'
    write_shard(path, [sample(i, i % 2) for i in range(4)], capacity=8)

    category_file = path / 'shard-00000/category.npy'
    converted = np.load(category_file).astype(layout)
    np.save(category_file, converted)
    manifest = json.loads((path / 'manifest.json').read_text())
    for shard in manifest['shards']:
        for entry in shard['files']:
            if entry['path'] == 'shard-00000/category.npy':
                entry['sha256'] = sha(path / entry['path'])
                entry['bytes'] = (path / entry['path']).stat().st_size
    atomic_json(path / 'manifest.json', manifest)

    dataset = ShardDataset(path, verify=True)
    batch = next(dataset.batches(shuffle=False))
    assert batch['category'].dtype == np.dtype(layout)
    assert batch['category'].shape == (4, 128)
    assert batch['category'][0, 0] == 7
