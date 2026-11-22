import inspect
import os
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest

from orchestration.common import ROOT, encode, now, ManifestError
from publication import retention
from publication.retention import plan, protected_path


def test_reference_protection_respects_path_components():
    roots = {'/behavior/v1/source/batch-a', '/behavior/v1/models/experiment-a'}
    assert protected_path('/behavior/v1/source/batch-a/raw.csv', roots)
    assert protected_path('/behavior/v1/models/experiment-a', roots)
    assert not protected_path('/behavior/v1/source/batch-ab/tmp', roots)
    with pytest.raises(ManifestError):
        protected_path('/behavior/v1/source/batch-a/../batch-b', roots)


class Cursor:
    def __init__(self, data):
        self.data = data
        self.rows = []

    def execute(self, sql, args=()):
        if 'FROM current_release' in sql:
            self.rows = [{'release_id': 'rel-1'}]
        elif 'FROM data_release' in sql:
            self.rows = self.data['releases']
        elif 'FROM task_run' in sql:
            self.rows = self.data['tasks']
        elif 'FROM data_batch' in sql:
            self.rows = self.data['sources']
        elif 'FROM experiment' in sql:
            self.rows = self.data['experiments']
        elif 'FROM model_run' in sql:
            self.rows = self.data['model_runs']
        else:
            raise AssertionError(sql)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class Db:
    def __init__(self, data):
        self.data = data

    @contextmanager
    def cursor(self):
        yield Cursor(self.data)


def fake_hdfs_output():
    return SimpleNamespace(stdout=(
        b'/behavior/v1/raw/.source-active-1.tmp\n'
        b'/behavior/v1/releases/rel-1/.source-pinned.tmp\n'
        b'/behavior/v1/tmp/orphan-1.tmp\n'
        b'/behavior/v1/raw/data.csv\n'
    ))


@pytest.fixture
def retention_tree(tmp_path, monkeypatch):
    monkeypatch.setattr(retention, 'ROOT', tmp_path)
    for folder in ('var/downloads', 'var/recovery', 'var/scale/pinned',
                   'var/backups/release-old', 'var/backups/release-new',
                   'var/production/backups', 'var/production/models/exp-1/run-1'):
        (tmp_path / folder).mkdir(parents=True)
    (tmp_path / 'var/downloads/cache.bin').write_bytes(b'x' * 16)
    (tmp_path / 'var/recovery/old').mkdir()
    (tmp_path / 'var/production/backups/pre-migration-old.sql').write_text('dump')
    (tmp_path / 'var/backups/release-old/marker').write_text('old')
    (tmp_path / 'var/backups/release-new/marker').write_text('new')
    os.utime(tmp_path / 'var/backups/release-old', (1, 1))
    os.utime(tmp_path / 'var/backups/release-new', (2, 2))
    run_dir = tmp_path / 'var/production/models/exp-1/run-1'
    (run_dir / 'epoch-01-state.pt').write_text('state')
    (run_dir / 'epoch-02-validation.npy').write_text('kept')
    manifest = encode(dict(models_archive=str(tmp_path / 'var/scale/pinned'),
                           forecast_archive='/behavior/v1/releases/rel-1'))
    data = dict(
        releases=[dict(release_id='rel-1', batch_id='batch-1', experiment_id='exp-1', manifest_json=manifest)],
        tasks=[
            dict(run_id='active-1', batch_id='batch-1', status='RUNNING',
                 output_path='/behavior/v1/runs/active-1/out', finished_at=None),
            dict(run_id='old-task', batch_id='batch-2', status='FAILED',
                 output_path='/behavior/v1/runs/old-task/out', finished_at=now() - timedelta(days=10)),
        ],
        sources=[],
        experiments=[],
        model_runs=[dict(model_run_id='run-1', status='SUCCEEDED', checkpoint_path=None,
                         resume_path=None, best_epoch=2)],
    )
    config = dict(source=dict(path=str(tmp_path / 'var/data/source.csv')), hdfs=dict(root='/behavior/v1'))
    return Db(data), config


def test_plan_is_read_only_and_classifies_candidates_and_blocked(retention_tree, monkeypatch):
    assert list(inspect.signature(plan).parameters) == ['db', 'config']

    class Hdfs:
        def __init__(self, config, lease=None):
            pass

        def command(self, *args):
            return fake_hdfs_output()

    monkeypatch.setattr(retention, 'Hdfs', Hdfs)
    db, config = retention_tree
    result = plan(db, config)
    candidates = {item['path']: item['kind'] for item in result['candidates']}
    blocked = {item['path']: item['reason'] for item in result['blocked']}

    assert result['status'] == 'PASS' and result['mode'] == 'DRY_RUN'
    assert result['dry_run'] is True and result['deletion_authorized'] is False
    assert result['current_release_id'] == 'rel-1'
    assert 'deleted' not in result
    assert candidates[str(retention.ROOT / 'var/downloads/cache.bin')] == 'local'
    assert candidates[str(retention.ROOT / 'var/recovery/old')] == 'local'
    assert candidates['/behavior/v1/tmp/orphan-1.tmp'] == 'hdfs'
    assert any(path.endswith('.old-task.tmp') and kind == 'hdfs' for path, kind in candidates.items())
    assert any(path.endswith('pre-migration-old.sql') for path in candidates)
    assert any(path.endswith('epoch-01-state.pt') for path in candidates)
    assert not any(path.endswith('epoch-02-validation.npy') for path in candidates)
    assert blocked['/behavior/v1/raw/.source-active-1.tmp'] == 'active task temporary'
    assert blocked['/behavior/v1/releases/rel-1/.source-pinned.tmp'] == 'referenced or active'
    assert blocked[str(retention.ROOT / 'var/scale/pinned')] == 'referenced or active'
    assert result['errors'] == []


def test_plan_records_hdfs_scan_errors_instead_of_swallowing_them(retention_tree, monkeypatch):
    class Hdfs:
        def __init__(self, config, lease=None):
            pass

        def command(self, *args):
            raise OSError('hdfs unavailable')

    monkeypatch.setattr(retention, 'Hdfs', Hdfs)
    db, config = retention_tree
    result = plan(db, config)
    assert result['errors'] == [dict(kind='hdfs', scope='/behavior/v1', error='hdfs unavailable')]
    assert any(item['path'].endswith('cache.bin') for item in result['candidates'])
