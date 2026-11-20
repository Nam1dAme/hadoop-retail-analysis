"""Validation-only selection, sealed test evaluation, and immutable model archive."""
import json
from pathlib import Path
import time
import uuid
import numpy as np
import torch
from orchestration.common import ROOT, atomic_json, fingerprint, sha, now, stamp, encode, commit, ManifestError
from orchestration.storage import Hdfs
from .models import seed_all, make_model, rule_group_codes, NUMERIC_DIM
from .training import fit, score, record_metrics, trusted_load, hardware
from .metrics import choose_threshold, evaluate_file
from .shards import ShardDataset
from .benchmark import run_benchmark

ALGORITHM_ORDER = {'CONSTANT': 0, 'LOGISTIC': 1, 'GRU': 2, 'RULE': 3}


def _selection_key(item):
    values = item['validation_metrics']['ALL']
    logloss = values.get('logloss')
    return (float('inf') if logloss is None else float(logloss), int(item['parameter_count']),
            ALGORITHM_ORDER.get(item.get('algorithm'), len(ALGORITHM_ORDER)))


def select(candidates):
    """Best VALIDATION AP; ties: logloss, parameter count, predefined algorithm order."""
    best = candidates[0]
    for item in candidates[1:]:
        x = item['validation_metrics']['ALL'].get('ap')
        y = best['validation_metrics']['ALL'].get('ap')
        if x is None:
            continue
        if y is None or x > y + 1e-6 or (abs(x - y) <= 1e-6 and _selection_key(item) < _selection_key(best)):
            best = item
    return best


def constant(lease, experiment, train, validation, directory, sample_hash, seed=42):
    positive = train.manifest['positive_count']
    total = train.manifest['rows']
    rate = positive/total
    identity = dict(algorithm='CONSTANT', seed=seed, sample_hash=sample_hash, positive_count=positive, sample_count=total, rate=rate)
    with lease.db.cursor() as q:
        q.execute('SELECT * FROM model_run WHERE experiment_id=%s AND algorithm=%s AND config_hash=%s',
                  (experiment, 'CONSTANT', fingerprint(identity)))
        row = q.fetchone()
        if row and row['status'] == 'SUCCEEDED':
            p = Path(row['checkpoint_path'])
            if sha(p) != row['checkpoint_hash']:
                raise ManifestError('Constant rules changed')
            return json.loads((p.parent/'manifest.json').read_text())
        model_id = row['model_run_id'] if row else str(uuid.uuid4())
        local = Path(directory)/model_id
        local.mkdir(parents=True, exist_ok=True)
        if not row:
            q.execute('INSERT INTO model_run(model_run_id,experiment_id,algorithm,status,seed,config_hash,config_json,device,parameter_count,hardware_json,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s)',
                      (model_id, experiment, 'CONSTANT', 'RUNNING', seed, fingerprint(identity), encode(identity), 'cpu', '{}', now(), now()))
    atomic_json(local/'rules.json', identity)
    score(validation, None, torch.device('cpu'), 'CONSTANT', local/'validation.npy', rate=rate, lease=lease)
    data = np.load(local/'validation.npy', mmap_mode='r', allow_pickle=False)
    threshold = choose_threshold(data['score'], data['label'])
    values = evaluate_file(local/'validation.npy', threshold)
    atomic_json(local/'validation_metrics.json', values)
    record_metrics(lease.db, model_id, 'VALIDATION', values, sha(local/'validation.npy'))
    result = dict(status='SUCCEEDED', model_run_id=model_id, experiment_id=experiment, algorithm='CONSTANT',
                  config=identity, code_commit=commit(), sample_hash=sample_hash,
                  checkpoint_path=str(local/'rules.json'), checkpoint_hash=sha(local/'rules.json'),
                  validation_scores=str(local/'validation.npy'), parameter_count=0,
                  threshold=threshold, validation_metrics=values, training_seconds=0.)
    result['outputs'] = [dict(path=p.name, sha256=sha(p), bytes=p.stat().st_size) for p in sorted(local.iterdir()) if p.name != 'manifest.json']
    atomic_json(local/'manifest.json', result)
    with lease.db.cursor() as q:
        q.execute('UPDATE model_run SET status=%s,checkpoint_path=%s,checkpoint_hash=%s,threshold=%s,training_seconds=0,updated_at=%s WHERE model_run_id=%s',
                  ('SUCCEEDED', result['checkpoint_path'], result['checkpoint_hash'], threshold, now(), model_id))
    return result


def rule_counts(train, mean, std):
    """TRAIN counts/positives per binary-signal group; no labels from VALIDATION/TEST are used."""
    counts = np.zeros(16, dtype=np.int64)
    positives = np.zeros(16, dtype=np.int64)
    for batch in train.batches(512, shuffle=False, columns=('numeric', 'label')):
        codes = rule_group_codes(batch['numeric'], mean, std)
        labels = batch['label'].astype(np.int64)
        counts += np.bincount(codes, minlength=16)
        positives += np.bincount(codes, weights=labels, minlength=16).astype(np.int64)
    return counts, positives


def rule(lease, experiment, train, validation, directory, sample_hash, seed=42):
    """16-group smoothed empirical-rate baseline on de-standardized TRAIN numeric features."""
    normalization_path = train.path.parent/'normalization.json'
    if not normalization_path.is_file():
        raise ManifestError('RULE baseline requires TRAIN normalization')
    normalization = json.loads(normalization_path.read_text())
    mean, std = normalization['mean'], normalization['std']
    if len(mean) != NUMERIC_DIM or len(std) != NUMERIC_DIM or any(float(v) <= 0 for v in std):
        raise ManifestError('Invalid normalization for RULE baseline')
    counts, positives = rule_counts(train, mean, std)
    table = {f'{code:04b}': (int(positives[code]) + 1) / (int(counts[code]) + 2) for code in range(16)}
    identity = dict(schema_version='rule-v1', algorithm='RULE', seed=seed, sample_hash=sample_hash,
                    rule=dict(table=table, mean=mean, std=std),
                    counts=counts.tolist(), positives=positives.tolist())
    config_hash = fingerprint(identity)
    with lease.db.cursor() as q:
        q.execute('SELECT * FROM model_run WHERE experiment_id=%s AND algorithm=%s AND config_hash=%s',
                  (experiment, 'RULE', config_hash))
        row = q.fetchone()
        if row and row['status'] == 'SUCCEEDED':
            p = Path(row['checkpoint_path'])
            if sha(p) != row['checkpoint_hash']:
                raise ManifestError('Rule table changed')
            return json.loads((p.parent/'manifest.json').read_text())
        model_id = row['model_run_id'] if row else str(uuid.uuid4())
        local = Path(directory)/model_id
        local.mkdir(parents=True, exist_ok=True)
        if not row:
            q.execute('INSERT INTO model_run(model_run_id,experiment_id,algorithm,status,seed,config_hash,config_json,device,parameter_count,hardware_json,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s)',
                      (model_id, experiment, 'RULE', 'RUNNING', seed, config_hash, encode(identity), 'cpu', '{}', now(), now()))
    atomic_json(local/'rules.json', identity)
    score(validation, None, torch.device('cpu'), 'RULE', local/'validation.npy', rule=identity['rule'], lease=lease)
    data = np.load(local/'validation.npy', mmap_mode='r', allow_pickle=False)
    threshold = choose_threshold(data['score'], data['label'])
    values = evaluate_file(local/'validation.npy', threshold)
    atomic_json(local/'validation_metrics.json', values)
    record_metrics(lease.db, model_id, 'VALIDATION', values, sha(local/'validation.npy'))
    result = dict(status='SUCCEEDED', model_run_id=model_id, experiment_id=experiment, algorithm='RULE',
                  config=identity, code_commit=commit(), sample_hash=sample_hash,
                  checkpoint_path=str(local/'rules.json'), checkpoint_hash=sha(local/'rules.json'),
                  validation_scores=str(local/'validation.npy'), parameter_count=0,
                  threshold=threshold, validation_metrics=values, training_seconds=0.)
    result['outputs'] = [dict(path=p.name, sha256=sha(p), bytes=p.stat().st_size)
                         for p in sorted(local.iterdir()) if p.name != 'manifest.json']
    atomic_json(local/'manifest.json', result)
    with lease.db.cursor() as q:
        q.execute('UPDATE model_run SET status=%s,checkpoint_path=%s,checkpoint_hash=%s,threshold=%s,training_seconds=0,updated_at=%s WHERE model_run_id=%s',
                  ('SUCCEEDED', result['checkpoint_path'], result['checkpoint_hash'], threshold, now(), model_id))
    return result


def run_experiment(lease, sample_index, benchmark=True):
    sample_index = Path(sample_index)
    source = json.loads(sample_index.read_text())
    experiment = source['experiment_id']
    directory = ROOT/'var/production/models'/experiment
    directory.mkdir(parents=True, exist_ok=True)
    cache = Path(source['samples']['cache_path'])
    with lease.db.cursor() as q:
        q.execute('SELECT * FROM experiment WHERE experiment_id=%s', (experiment,))
        row = q.fetchone()
        if not row or row['batch_id'] != source['batch_id']:
            raise ManifestError('Experiment/sample identity mismatch')
    if row['status'] == 'SUCCEEDED':
        result = json.loads((directory/'manifest.json').read_text())
        if result['sample_hash'] != row['sample_hash']:
            raise ManifestError('Experiment input changed')
        return result
    seed_all()
    started = time.monotonic()
    # Bind local shards to the authoritative export rather than trusting editable local manifests.
    storage = Hdfs(lease.config, lease)
    export = storage.json(row['sample_manifest'])
    if export['stage'] != 'E01' or export['batch_id'] != source['batch_id'] or fingerprint(export['outputs']) != row['sample_hash']:
        raise ManifestError('Authoritative sample export identity changed')
    prefix = source['samples']['archive_path']+'/'
    for file in export['outputs']:
        if file['path'].startswith(prefix):
            local_file = cache/file['path'][len(prefix):]
            if not local_file.is_file() or sha(local_file) != file['sha256']:
                raise ManifestError('Local sample differs from authoritative export')
    if row['test_sealed_at'] is None:
        train = ShardDataset(cache/'TRAIN', verify=True)
        validation = ShardDataset(cache/'VALIDATION', verify=True)
        with lease.db.cursor() as q:
            q.execute('UPDATE experiment SET status=%s,updated_at=%s WHERE experiment_id=%s', ('RUNNING', now(), experiment))
        if benchmark:
            run_benchmark(lease, train, experiment, directory)
        candidates = [constant(lease, experiment, train, validation, directory, row['sample_hash']),
                      rule(lease, experiment, train, validation, directory, row['sample_hash'])]
        for algorithm in ('LOGISTIC', 'GRU'):
            for rate in (.001, .0003):
                candidates.append(fit(lease, experiment, train, validation, algorithm, rate, directory, row['sample_hash']))
        finalists = [select([x for x in candidates if x['algorithm'] == algorithm])
                     for algorithm in ('CONSTANT', 'LOGISTIC', 'GRU', 'RULE')]
        winner = select(finalists)
        # Extra seeds reuse each finalist's frozen hyperparameters and never enter selection.
        extra_runs = [fit(lease, experiment, train, validation, item['algorithm'],
                          item['config']['learning_rate'], directory, row['sample_hash'], seed=seed)
                      for item in finalists if item['algorithm'] in ('LOGISTIC', 'GRU') for seed in (43, 44)]
        selection = dict(selected_model_id=winner['model_run_id'], finalists=finalists, candidates=candidates,
                         seed_variance_runs=extra_runs,
                         reason='VALIDATION ALL AP; ties: LogLoss, parameter count, predefined order',
                         sealed_at=stamp(), sample_hash=row['sample_hash'], code_commit=commit())
        atomic_json(directory/'selection.json', selection)
        with lease.db.cursor() as q:
            q.execute('UPDATE experiment SET status=%s,selected_model_id=%s,test_sealed_at=%s,selection_json=%s,updated_at=%s WHERE experiment_id=%s AND test_sealed_at IS NULL',
                      ('SEALED', winner['model_run_id'], now(), encode(selection), now(), experiment))
            if q.rowcount != 1:
                raise ManifestError('Test seal changed concurrently')
    else:
        selection = json.loads(row['selection_json'])
        if selection['sample_hash'] != row['sample_hash']:
            raise ManifestError('Sealed experiment input changed')
    # No training or model/threshold selection occurs after this point.
    test = ShardDataset(cache/'TEST', verify=True)
    finalist_ids = {item['model_run_id'] for item in selection['finalists']}
    evaluation = []
    seed_observations = []
    for item in list(selection['finalists']) + list(selection.get('seed_variance_runs', [])):
        local = Path(item['checkpoint_path']).parent
        test_path = local/'test.npy'
        metrics_path = local/'test_metrics.json'
        test_manifest = local/'test-manifest.json'
        if test_manifest.exists():
            saved = json.loads(test_manifest.read_text())
            if saved['sha256'] != sha(test_path) or saved['checkpoint_hash'] != item['checkpoint_hash'] or saved['threshold'] != item['threshold']:
                raise ManifestError('Sealed test artifact changed')
            values = json.loads(metrics_path.read_text())
        else:
            algorithm = item['algorithm']
            model = None
            device = torch.device('cuda' if algorithm == 'GRU' else 'cpu')
            if algorithm in ('LOGISTIC', 'GRU'):
                model = make_model(algorithm).to(device)
                model.load_state_dict(trusted_load(item['checkpoint_path'], item['checkpoint_hash'], directory))
            rule_config = item['config'].get('rule') if algorithm == 'RULE' else None
            if algorithm == 'RULE' and not rule_config:
                raise ManifestError('RULE test scoring requires the frozen rule table')
            result = score(test, model, device, algorithm, test_path,
                           rate=item['config'].get('rate'), rule=rule_config, lease=lease)
            values = evaluate_file(test_path, item['threshold'])
            atomic_json(metrics_path, values)
            atomic_json(test_manifest, dict(**result, checkpoint_hash=item['checkpoint_hash'], threshold=item['threshold']))
            del model
            if device.type == 'cuda':
                torch.cuda.empty_cache()
        record_metrics(lease.db, item['model_run_id'], 'TEST', values, sha(test_path))
        seed_observations.append((item, values))
        if item['model_run_id'] in finalist_ids:
            evaluation.append(dict(model_run_id=item['model_run_id'], algorithm=item['algorithm'], metrics=values,
                                   scores=str(test_path), sha256=sha(test_path), threshold=item['threshold']))
    seed_variance = {}
    for algorithm in ('LOGISTIC', 'GRU'):
        observed = [(item['config']['seed'], values['ALL']['ap'], values['ALL']['roc_auc'])
                    for item, values in seed_observations if item['algorithm'] == algorithm]
        observed.sort(key=lambda row: row[0])
        if not observed:
            continue
        aps = [row[1] for row in observed if row[1] is not None]
        seed_variance[algorithm] = dict(
            seeds=[row[0] for row in observed], test_ap=[row[1] for row in observed],
            test_auc=[row[2] for row in observed],
            mean=float(np.mean(aps)) if aps else None, std=float(np.std(aps)) if aps else None)
    # Publish model evidence only after all required training and sealed evaluation succeeded.
    storage = Hdfs(lease.config, lease)
    remote = f'{lease.config["hdfs"]["root"]}/models/experiment_id={experiment}'
    outputs = [dict(path=str(p.relative_to(directory)), sha256=sha(p), bytes=p.stat().st_size)
               for p in sorted(directory.rglob('*')) if p.is_file() and p.name != 'manifest.json' and p.name != 'progress.json']
    # Include each model's own manifest, exclude only the experiment root manifest.
    for p in directory.glob('*/manifest.json'):
        outputs.append(dict(path=str(p.relative_to(directory)), sha256=sha(p), bytes=p.stat().st_size))
    result = dict(status='SUCCEEDED', experiment_id=experiment, batch_id=source['batch_id'],
                  sample_hash=row['sample_hash'], normalization_hash=row['normalization_hash'],
                  code_commit=commit(), environment_hash=sha(ROOT/'config/environment.lock.json'),
                  selected_model_id=selection['selected_model_id'], selection=selection, test=evaluation,
                  seed_variance=seed_variance, seconds=time.monotonic()-started, archive=remote, outputs=outputs)
    atomic_json(directory/'manifest.json', result)
    if not storage.exists(remote):
        staged = remote+'.staging-'+str(uuid.uuid4())
        storage.mkdir(str(Path(remote).parent))
        storage.put(directory, staged)
        for f in outputs:
            if storage.digest(staged+'/'+f['path']) != (f['sha256'], f['bytes']):
                raise ManifestError('Archived model artifact differs')
        storage.rename(staged, remote)
    else:
        archived = storage.json(remote+'/manifest.json')
        if archived != result:
            raise ManifestError('Existing immutable model archive differs')
    with lease.db.cursor() as q:
        q.execute('UPDATE experiment SET status=%s,updated_at=%s WHERE experiment_id=%s', ('SUCCEEDED', now(), experiment))
    atomic_json(ROOT/'var/production/models.json', result)
    return result
