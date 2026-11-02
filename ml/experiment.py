"""Validation-only selection, sealed test evaluation, and immutable model archive."""
import json
from pathlib import Path
import time
import uuid
import numpy as np
import torch
from orchestration.common import ROOT, atomic_json, fingerprint, sha, now, stamp, encode, commit, ManifestError
from orchestration.storage import Hdfs
from .models import seed_all, make_model
from .training import fit, score, record_metrics, trusted_load, hardware
from .metrics import choose_threshold, evaluate_file
from .shards import ShardDataset
from .benchmark import run_benchmark


def select(candidates):
    best = candidates[0]
    for item in candidates[1:]:
        x, y = item['validation_metrics']['ALL'], best['validation_metrics']['ALL']
        if x['ap'] > y['ap']+1e-6 or (abs(x['ap']-y['ap']) <= 1e-6 and
                (x['logloss'], item['parameter_count']) < (y['logloss'], best['parameter_count'])):
            best = item
    return best


def constant(lease, experiment, train, validation, directory, sample_hash):
    positive = train.manifest['positive_count']
    total = train.manifest['rows']
    rate = positive/total
    identity = dict(algorithm='CONSTANT', sample_hash=sample_hash, positive_count=positive, sample_count=total, rate=rate)
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
            q.execute('INSERT INTO model_run(model_run_id,experiment_id,algorithm,status,seed,config_hash,config_json,device,parameter_count,hardware_json,created_at,updated_at) VALUES(%s,%s,%s,%s,42,%s,%s,%s,0,%s,%s,%s)',
                      (model_id, experiment, 'CONSTANT', 'RUNNING', fingerprint(identity), encode(identity), 'cpu', '{}', now(), now()))
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
        candidates = [constant(lease, experiment, train, validation, directory, row['sample_hash'])]
        for algorithm in ('LOGISTIC', 'GRU'):
            for rate in (.001, .0003):
                candidates.append(fit(lease, experiment, train, validation, algorithm, rate, directory, row['sample_hash']))
        finalists = [select([x for x in candidates if x['algorithm'] == algorithm]) for algorithm in ('CONSTANT', 'LOGISTIC', 'GRU')]
        winner = select(finalists)
        selection = dict(selected_model_id=winner['model_run_id'], finalists=finalists, candidates=candidates,
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
    evaluation = []
    for item in selection['finalists']:
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
            if algorithm != 'CONSTANT':
                model = make_model(algorithm).to(device)
                model.load_state_dict(trusted_load(item['checkpoint_path'], item['checkpoint_hash'], directory))
            result = score(test, model, device, algorithm, test_path,
                           rate=item['config'].get('rate'), lease=lease)
            values = evaluate_file(test_path, item['threshold'])
            atomic_json(metrics_path, values)
            atomic_json(test_manifest, dict(**result, checkpoint_hash=item['checkpoint_hash'], threshold=item['threshold']))
            del model
            if device.type == 'cuda':
                torch.cuda.empty_cache()
        record_metrics(lease.db, item['model_run_id'], 'TEST', values, sha(test_path))
        evaluation.append(dict(model_run_id=item['model_run_id'], algorithm=item['algorithm'], metrics=values,
                               scores=str(test_path), sha256=sha(test_path), threshold=item['threshold']))
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
                  seconds=time.monotonic()-started, archive=remote, outputs=outputs)
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
