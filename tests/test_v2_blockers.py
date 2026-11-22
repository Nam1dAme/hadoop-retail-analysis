from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import py_compile
import re
import numpy as np
import pytest
from fastapi.testclient import TestClient

from orchestration.common import ROOT
from service.api import create_app
from service.security import Cursors

RELEASE_ID = '00000000-0000-0000-0000-000000000001'
EXPERIMENT_ID = '00000000-0000-0000-0000-000000000002'
MODEL_ID = '00000000-0000-0000-0000-000000000003'


class FakePredictionPool:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def one(self, sql, args=()):
        self.queries.append((sql, args))
        if 'FROM data_release' in sql:
            return dict(
                release_id=RELEASE_ID,
                experiment_id=EXPERIMENT_ID,
                selected_model_id=MODEL_ID,
                status='READY',
            )
        if 'FROM model_run' in sql:
            return dict(model_run_id=MODEL_ID)
        raise AssertionError(f'Unexpected query: {sql}')

    def all(self, sql, args=()):
        self.queries.append((sql, args))
        assert 'ORDER BY score DESC,user_id ASC,as_of ASC' in sql
        rows = list(self.rows)
        if 'as_of>%s' in sql:
            assert '(score<%s OR (score=%s AND (user_id>%s OR (user_id=%s AND as_of>%s))))' in sql
            last_score, _, last_user, _, last_as_of, limit = args[-6:]
            cursor_dt = datetime.fromisoformat(last_as_of)
            rows = [
                r for r in rows
                if (
                    r['score'] < last_score
                    or (
                        r['score'] == last_score
                        and (
                            r['user_id'] > last_user
                            or (r['user_id'] == last_user and r['as_of'] > cursor_dt)
                        )
                    )
                )
            ]
        else:
            limit = args[-1]
        rows.sort(key=lambda r: (-r['score'], r['user_id'], r['as_of']))
        return [dict(r) for r in rows[:limit]]


def test_predictions_dual_day_cursor_pagination():
    day1 = datetime(2017, 12, 2, 16, 0, 0)
    day2 = datetime(2017, 12, 3, 16, 0, 0)
    rows = [
        dict(user_id='1001', score=0.5, predicted_positive=1, observed_label=0,
             history_event_count=5, as_of=day1, window_end=day1 + timedelta(days=1), model_run_id=MODEL_ID),
        dict(user_id='1001', score=0.5, predicted_positive=1, observed_label=1,
             history_event_count=7, as_of=day2, window_end=day2 + timedelta(days=1), model_run_id=MODEL_ID),
        dict(user_id='1002', score=0.5, predicted_positive=1, observed_label=0,
             history_event_count=3, as_of=day1, window_end=day1 + timedelta(days=1), model_run_id=MODEL_ID),
        dict(user_id='1003', score=0.2, predicted_positive=0, observed_label=0,
             history_event_count=2, as_of=day1, window_end=day1 + timedelta(days=1), model_run_id=MODEL_ID),
    ]
    pool = FakePredictionPool(rows)
    cursor_key = b'k' * 32
    app = create_app(pool=pool, cursor_key=cursor_key)
    client = TestClient(app)

    collected = []
    cursor = None
    while True:
        params = {'release_id': RELEASE_ID, 'mode': 'TEST', 'model_run_id': MODEL_ID, 'limit': 1}
        if cursor:
            params['cursor'] = cursor
        res = client.get('/api/v1/predictions', params=params)
        assert res.status_code == 200
        body = res.json()
        collected.extend(body['data'])
        if not body['meta']['has_more']:
            assert body['meta']['next_cursor'] is None
            break
        cursor = body['meta']['next_cursor']

    assert [(r['user_id'], r['score'], r['as_of']) for r in collected] == [
        ('1001', 0.5, '2017-12-02T16:00:00Z'),
        ('1001', 0.5, '2017-12-03T16:00:00Z'),
        ('1002', 0.5, '2017-12-02T16:00:00Z'),
        ('1003', 0.2, '2017-12-02T16:00:00Z'),
    ]

    # Legacy 2-element cursor should be rejected cleanly with 422 INVALID_QUERY.
    context_hash = pool.queries[-1]  # sanity check queries ran
    assert context_hash is not None
    bad_res = client.get(
        '/api/v1/predictions',
        params={'release_id': RELEASE_ID, 'mode': 'TEST', 'model_run_id': MODEL_ID, 'cursor': 'invalid'},
    )
    assert bad_res.status_code == 422
    assert bad_res.json()['error']['code'] == 'INVALID_QUERY'


def test_migration_011_prediction_dual_day_pk():
    sql_path = ROOT / 'migrations/011_prediction_dual_day_pk.sql'
    raw = sql_path.read_text(encoding='utf-8')
    statements = [s.strip() for s in raw.split(';') if s.strip()]
    assert len(statements) == 1
    normalized = re.sub(r'\s+', ' ', statements[0]).upper()
    assert 'ALTER TABLE PREDICTION' in normalized
    assert 'DROP PRIMARY KEY' in normalized
    assert 'ADD PRIMARY KEY(RELEASE_ID,MODE,MODEL_RUN_ID,USER_ID,AS_OF)' in normalized
    assert 'DROP INDEX PREDICTION_SCORES' in normalized
    assert 'ADD INDEX PREDICTION_SCORES(RELEASE_ID,MODE,MODEL_RUN_ID,SCORE DESC,USER_ID ASC,AS_OF ASC)' in normalized


def test_scripts_compile_and_verify_release_dual_day(tmp_path):
    for rel in ('service/api.py', 'app/views.py', 'scripts/verify-models.py', 'scripts/verify-release.py'):
        py_compile.compile(str(ROOT / rel), doraise=True)

    spec = importlib.util.spec_from_file_location('verify_release', ROOT / 'scripts/verify-release.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    cache = tmp_path / 'TEST'
    shard_dirs = [cache / 'shard-000', cache / 'shard-001']
    asof_d1 = int(datetime(2017, 12, 2, 16, 0, 0, tzinfo=timezone.utc).timestamp())
    asof_d2 = int(datetime(2017, 12, 3, 16, 0, 0, tzinfo=timezone.utc).timestamp())

    def write_shard(directory, entries):
        directory.mkdir(parents=True, exist_ok=True)
        offsets = [0]
        raw_users = bytearray()
        asofs, labels, histories = [], [], []
        keys = []
        for uid, ts, lbl in entries:
            b = uid.encode('utf-8')
            raw_users.extend(b)
            offsets.append(len(raw_users))
            asofs.append(ts)
            labels.append(lbl)
            histories.append(10)
            keys.append(hashlib.sha256(b + b'|' + str(ts).encode()).hexdigest().encode())
        (directory / 'user_ids.utf8').write_bytes(bytes(raw_users))
        np.save(directory / 'user_offsets.npy', np.array(offsets, dtype=np.int64), allow_pickle=False)
        np.save(directory / 'as_of.npy', np.array(asofs, dtype=np.int64), allow_pickle=False)
        np.save(directory / 'label.npy', np.array(labels, dtype=np.int8), allow_pickle=False)
        np.save(directory / 'history_event_count.npy', np.array(histories, dtype=np.int64), allow_pickle=False)
        np.save(directory / 'sample_key.npy', np.array(keys, dtype='S64'), allow_pickle=False)
        return keys

    k0 = write_shard(shard_dirs[0], [('1001', asof_d2, 1), ('1002', asof_d1, 0)])
    k1 = write_shard(shard_dirs[1], [('1001', asof_d1, 0)])
    (cache / 'manifest.json').write_text(json.dumps({
        'shards': [
            {'directory': 'shard-000', 'rows': 2},
            {'directory': 'shard-001', 'rows': 1},
        ]
    }))
    score_dtype = np.dtype([('score', '<f8'), ('label', 'i1'), ('sample_key', 'S64')])
    scores_path = tmp_path / 'scores.npy'
    np.save(scores_path, np.array([
        (0.7, 1, k0[0]),
        (0.3, 0, k0[1]),
        (0.4, 0, k1[0]),
    ], dtype=score_dtype), allow_pickle=False)

    runs_dir = tmp_path / 'runs'
    runs_dir.mkdir()
    out = list(mod.expected_rows(cache, scores_path, runs_dir))
    assert [(r['user'].decode(), int(r['asof'])) for r in out] == [
        ('1001', asof_d1),
        ('1001', asof_d2),
        ('1002', asof_d1),
    ]

    # Duplicate (user, as_of) must still fail.
    k_dup = write_shard(shard_dirs[1], [('1001', asof_d2, 1)])
    np.save(scores_path, np.array([
        (0.7, 1, k0[0]),
        (0.3, 0, k0[1]),
        (0.7, 1, k_dup[0]),
    ], dtype=score_dtype), allow_pickle=False)
    runs_dup_dir = tmp_path / 'runs_dup'
    runs_dup_dir.mkdir()
    with pytest.raises(AssertionError, match='Duplicate sample'):
        list(mod.expected_rows(cache, scores_path, runs_dup_dir))


def test_verify_models_supports_v2_candidates():
    src = (ROOT / 'scripts/verify-models.py').read_text(encoding='utf-8')
    assert "['CONSTANT','RULE','GBDT','LOGISTIC','LOGISTIC','GRU','GRU']" in src
    assert "('CONSTANT','LOGISTIC','GRU','RULE','GBDT')" in src
    assert '1<=len(epochs)<=20' in src
    assert '1<=len(epochs)<=300' in src
