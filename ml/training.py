"""Full-population training, bounded scoring and epoch-boundary crash recovery."""
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import time
import uuid
import numpy as np
import torch
from torch.nn import functional as F
from orchestration.common import ROOT, atomic_json, encode, fingerprint, sha, now, stamp, commit, ManifestError, PipelineError
from .models import make_model, seed_all, tensors, FEATURES
from .shards import ShardDataset, training_groups
from .metrics import SCORE_DTYPE, evaluate_file, choose_threshold


def save_checkpoint(path, payload):
    path = Path(path)
    temp = path.with_suffix('.tmp')
    with temp.open('wb') as f:
        torch.save(payload, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)
    return sha(path)


def trusted_load(path, digest, directory):
    path = Path(path).resolve(strict=True)
    if not path.is_relative_to(Path(directory).resolve()) or sha(path) != digest:
        raise ManifestError('Untrusted or changed model checkpoint')
    return torch.load(path, map_location='cpu', weights_only=True)


def cpu_state(model):
    return {k: v.detach().cpu() for k, v in model.state_dict().items()}


def hardware(device):
    return {'device': str(device), 'name': torch.cuda.get_device_name(0) if device.type == 'cuda' else 'CPU',
            'torch': str(torch.__version__), 'cuda_runtime': torch.version.cuda,
            'cpu_threads': torch.get_num_threads(), 'tf32': False, 'deterministic': True,
            'total_gpu_mib': torch.cuda.get_device_properties(0).total_memory / 1024 ** 2 if device.type == 'cuda' else None}


def resource_usage(device):
    return {'rss_mib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
            'peak_allocated_mib': torch.cuda.max_memory_allocated() / 1024 ** 2 if device.type == 'cuda' else 0.,
            'peak_reserved_mib': torch.cuda.max_memory_reserved() / 1024 ** 2 if device.type == 'cuda' else 0.}


def resource_guard(device, lease=None):
    if lease:
        lease.guard()
    usage = resource_usage(device)
    if usage['rss_mib'] > 3072 or usage['peak_allocated_mib'] > 6144 or usage['peak_reserved_mib'] > 6656:
        raise PipelineError('Training resource budget exceeded: ' + encode(usage))


def score(dataset, model, device, algorithm, output, batch_size=512, rate=None, lease=None):
    """Write exactly one score per sample in immutable input order; evaluation is FP32."""
    output = Path(output)
    temporary = output.with_suffix('.tmp.npy')
    prediction = np.lib.format.open_memmap(temporary, mode='w+', dtype=SCORE_DTYPE,
                                         shape=(dataset.manifest['rows'],))
    columns = ('numeric',) if algorithm == 'LOGISTIC' else FEATURES if algorithm == 'GRU' else ()
    columns = (*columns, 'label', 'cohort', 'sample_key')
    if model is not None:
        model.eval()
    offset = 0
    digest = hashlib.sha256()
    with torch.no_grad():
        for batch in dataset.batches(batch_size, shuffle=False, columns=columns):
            resource_guard(device, lease)
            if algorithm == 'CONSTANT':
                values = np.full(len(batch['label']), rate, dtype=np.float64)
            else:
                inputs = tensors(batch, device, algorithm)
                values = torch.sigmoid(model(inputs).float()).cpu().numpy().astype(np.float64)
            if not np.isfinite(values).all():
                raise PipelineError('Non-finite model scores')
            end = offset + len(values)
            prediction['score'][offset:end] = values
            for key in ('label', 'cohort', 'sample_key'):
                prediction[key][offset:end] = batch[key]
            for key in batch['sample_key']:
                digest.update(bytes(key) + b'\n')
            offset = end
    if offset != dataset.manifest['rows'] or digest.hexdigest() != dataset.manifest['sample_order_sha256']:
        raise ManifestError('Prediction population/order changed')
    prediction.flush()
    del prediction
    os.replace(temporary, output)
    return {'path': str(output), 'sha256': sha(output), 'rows': offset, 'sample_order_sha256': digest.hexdigest()}


def record_metrics(db, model_id, split, result, evaluation_hash):
    with db.cursor() as q:
        for cohort, values in result.items():
            for name in ('ap', 'roc_auc', 'logloss', 'brier', 'precision', 'recall', 'f1', 'tp', 'fp', 'fn', 'tn'):
                q.execute('INSERT INTO model_metric VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value),sample_count=VALUES(sample_count),positive_count=VALUES(positive_count),reason=VALUES(reason),evaluation_hash=VALUES(evaluation_hash)',
                          (model_id, split, cohort, name, values.get(name), values['sample_count'], values['positive_count'], values.get('reason'), evaluation_hash))


def preflight(train, algorithm, device):
    if algorithm == 'GRU' and device.type != 'cuda':
        raise PipelineError('Formal GRU training requires CUDA')
    candidates = (256, 128, 64, 32) if device.type == 'cuda' else (256,)
    evidence = []
    for micro in candidates:
        model = optimizer = inputs = loss = None
        try:
            seed_all()
            if device.type == 'cuda':
                torch.cuda.empty_cache()
                torch.cuda.reset_peak_memory_stats()
            model = make_model(algorithm).to(device)
            optimizer = torch.optim.AdamW(model.parameters(), lr=.001, weight_decay=1e-4)
            batch = next(train.batches(micro, columns=(*FEATURES, 'label') if algorithm == 'GRU' else ('numeric', 'label')))
            inputs = tensors(batch, device, algorithm)
            scaler = torch.amp.GradScaler('cuda', enabled=device.type == 'cuda')
            for _ in range(3):
                optimizer.zero_grad(set_to_none=True)
                with torch.autocast(device.type, dtype=torch.float16, enabled=device.type == 'cuda'):
                    logits = model(inputs)
                loss = F.binary_cross_entropy_with_logits(logits.float(), inputs['label'])
                if not torch.isfinite(loss):
                    raise PipelineError('Preflight loss is non-finite')
                scaler.scale(loss).backward()
                scaler.unscale_(optimizer)
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1., error_if_nonfinite=True)
                scaler.step(optimizer)
                scaler.update()
            if device.type == 'cuda':
                torch.cuda.synchronize()
            usage = resource_usage(device)
            evidence.append(dict(micro_batch=micro, **usage))
            if usage['peak_allocated_mib'] <= 6144 and usage['peak_reserved_mib'] <= 6656 and usage['rss_mib'] <= 3072:
                return micro, evidence
        except torch.cuda.OutOfMemoryError:
            evidence.append(dict(micro_batch=micro, error='CUDA_OOM'))
        finally:
            del model, optimizer, inputs, loss
            if device.type == 'cuda':
                torch.cuda.empty_cache()
    raise PipelineError('Training preflight failed at minimum batch: ' + encode(evidence))


def fit(lease, experiment, train, validation, algorithm, learning_rate, directory, sample_hash):
    device = torch.device('cuda' if algorithm == 'GRU' else 'cpu')
    micro, preflight_evidence = preflight(train, algorithm, device)
    identity = dict(schema_version='model-v1', algorithm=algorithm, learning_rate=learning_rate,
                    weight_decay=1e-4, seed=42, effective_batch=512, micro_batch=micro,
                    max_epochs=10, patience=2, selection='VALIDATION_AP', sample_hash=sample_hash,
                    implementation=fingerprint({str(p.relative_to(ROOT)): sha(p) for p in sorted((ROOT/'ml').glob('*.py'))}))
    config_hash = fingerprint(identity)
    with lease.db.cursor() as q:
        q.execute('SELECT * FROM model_run WHERE experiment_id=%s AND algorithm=%s AND config_hash=%s',
                  (experiment, algorithm, config_hash))
        row = q.fetchone()
        if row and row['status'] == 'SUCCEEDED':
            path = Path(row['checkpoint_path'])
            trusted_load(path, row['checkpoint_hash'], directory)
            result = json.loads((path.parent/'manifest.json').read_text())
            for file in result['outputs']:
                if sha(path.parent/file['path']) != file['sha256']:
                    raise ManifestError('Successful model artifact changed')
            return result
        model_id = row['model_run_id'] if row else str(uuid.uuid4())
        local = Path(directory)/model_id
        local.mkdir(parents=True, exist_ok=True)
        seed_all()
        model = make_model(algorithm).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate, weight_decay=1e-4)
        scaler = torch.amp.GradScaler('cuda', enabled=device.type == 'cuda')
        hw = hardware(device)
        if row:
            q.execute('UPDATE model_run SET status=%s,updated_at=%s WHERE model_run_id=%s', ('RUNNING', now(), model_id))
        else:
            q.execute('INSERT INTO model_run(model_run_id,experiment_id,algorithm,status,seed,config_hash,config_json,device,parameter_count,hardware_json,created_at,updated_at) VALUES(%s,%s,%s,%s,42,%s,%s,%s,%s,%s,%s,%s)',
                      (model_id, experiment, algorithm, 'RUNNING', config_hash, encode(identity), str(device), sum(p.numel() for p in model.parameters()), encode(hw), now(), now()))
    atomic_json(local/'config.json', identity)
    atomic_json(local/'preflight.json', preflight_evidence)
    history = []
    best_ap = -1.
    stale = 0
    first_epoch = 1
    elapsed_before = 0.
    if row and row['resume_path']:
        checkpoint = trusted_load(row['resume_path'], row['resume_hash'], local)
        if checkpoint['config_hash'] != config_hash:
            raise ManifestError('Resume configuration differs')
        model.load_state_dict(checkpoint['model'])
        optimizer.load_state_dict(checkpoint['optimizer'])
        scaler.load_state_dict(checkpoint['scaler'])
        for state in optimizer.state.values():
            for key, value in state.items():
                if isinstance(value, torch.Tensor):
                    state[key] = value.to(device)
        torch.set_rng_state(checkpoint['torch_rng'])
        if device.type == 'cuda':
            torch.cuda.set_rng_state_all(checkpoint['cuda_rng'])
        random.setstate(checkpoint['python_rng'])
        numpy_rng = checkpoint['numpy_rng']
        np.random.set_state((numpy_rng[0], np.asarray(numpy_rng[1], dtype=np.uint32),
                             numpy_rng[2], numpy_rng[3], numpy_rng[4]))
        history = checkpoint['history']
        best_ap, stale = checkpoint['best_ap'], checkpoint['stale']
        elapsed_before = checkpoint['elapsed']
        first_epoch = checkpoint['epoch'] + 1
    started = time.monotonic()
    if device.type == 'cuda':
        torch.cuda.reset_peak_memory_stats()
    try:
        for epoch in range(first_epoch, 11):
            if stale >= 2:
                break
            epoch_started = time.monotonic()
            model.train()
            total = 0
            loss_total = 0.
            skipped = 0
            columns = (*FEATURES, 'label') if algorithm == 'GRU' else ('numeric', 'label')
            for index, (group, weights, count) in enumerate(training_groups(train, micro, 512, epoch=epoch, columns=columns)):
                resource_guard(device, lease)
                optimizer.zero_grad(set_to_none=True)
                for batch, weight in zip(group, weights):
                    inputs = tensors(batch, device, algorithm)
                    with torch.autocast(device.type, dtype=torch.float16, enabled=device.type == 'cuda'):
                        logits = model(inputs)
                    loss = F.binary_cross_entropy_with_logits(logits.float(), inputs['label'])
                    if not torch.isfinite(loss):
                        raise PipelineError('Non-finite training loss')
                    loss_total += loss.detach().item() * len(batch['label'])
                    scaler.scale(loss * weight).backward()
                scaler.unscale_(optimizer)
                # GradScaler handles overflow; finite gradients are clipped after unscale.
                norm = torch.nn.utils.clip_grad_norm_(model.parameters(), 1.)
                previous_scale = scaler.get_scale()
                scaler.step(optimizer)
                scaler.update()
                if scaler.get_scale() < previous_scale:
                    skipped += 1
                elif not torch.isfinite(norm):
                    raise PipelineError('Non-finite unscaled gradient')
                total += count
                if index % 200 == 0:
                    atomic_json(local/'progress.json', dict(status='RUNNING', algorithm=algorithm, epoch=epoch, samples=total,
                                train_rows=train.manifest['rows'], updated_at=stamp(), **resource_usage(device)))
                if index % 1000 == 0:
                    print(encode(dict(model=model_id, algorithm=algorithm, epoch=epoch, samples=total)), flush=True)
            if total != train.manifest['rows']:
                raise ManifestError('Incomplete training epoch')
            score(validation, model, device, algorithm, local/'validation-current.npy', lease=lease)
            values = evaluate_file(local/'validation-current.npy')
            ap = values['ALL']['ap']
            if ap is None:
                raise PipelineError('Validation must contain both classes')
            improved = ap > best_ap + 1e-6
            stale = 0 if improved else stale + 1
            if improved:
                best_ap = ap
                # Epoch-named files avoid overwriting a DB-referenced checkpoint before commit.
                best_path = local/f'epoch-{epoch:02}-state.pt'
                best_hash = save_checkpoint(best_path, cpu_state(model))
                os.replace(local/'validation-current.npy', local/f'epoch-{epoch:02}-validation.npy')
            else:
                (local/'validation-current.npy').unlink()
            entry = dict(epoch=epoch, train_loss=loss_total/total, validation_ap=ap,
                         validation_logloss=values['ALL']['logloss'], duration_seconds=time.monotonic()-epoch_started,
                         sample_count=total, skipped_updates=skipped, peak_gpu_mib=resource_usage(device)['peak_allocated_mib'])
            history.append(entry)
            resume_path = local/f'epoch-{epoch:02}-resume.pt'
            elapsed = elapsed_before + time.monotonic()-started
            numpy_rng = np.random.get_state()
            resume_hash = save_checkpoint(resume_path, dict(model=cpu_state(model), optimizer=optimizer.state_dict(),
                         scaler=scaler.state_dict(), torch_rng=torch.get_rng_state(),
                         cuda_rng=torch.cuda.get_rng_state_all() if device.type == 'cuda' else [],
                         python_rng=random.getstate(), numpy_rng=(numpy_rng[0], numpy_rng[1].tolist(), *numpy_rng[2:]),
                         config_hash=config_hash, epoch=epoch, history=history,
                         best_ap=best_ap, stale=stale, elapsed=elapsed))
            # Complete epoch, pointers and best selection become visible in one transaction.
            lease.db.begin()
            try:
                with lease.db.cursor() as q:
                    q.execute('INSERT INTO model_epoch VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                              (model_id, epoch, entry['train_loss'], ap, entry['validation_logloss'], entry['duration_seconds'], total, entry['peak_gpu_mib'], skipped))
                    q.execute('UPDATE model_run SET resume_path=%s,resume_hash=%s,training_seconds=%s,updated_at=%s WHERE model_run_id=%s',
                              (str(resume_path), resume_hash, elapsed, now(), model_id))
                    if improved:
                        q.execute('UPDATE model_run SET checkpoint_path=%s,checkpoint_hash=%s,best_epoch=%s WHERE model_run_id=%s',
                                  (str(best_path), best_hash, epoch, model_id))
                lease.db.commit()
            except BaseException:
                lease.db.rollback()
                raise
            atomic_json(local/'history.json', history)
            print(encode(dict(model=model_id, **entry)), flush=True)
            if os.environ.get('BEHAVIOR_TEST_STOP_AFTER_EPOCH') == str(epoch):
                raise PipelineError('Explicit epoch-boundary recovery test interruption')
        with lease.db.cursor() as q:
            q.execute('SELECT * FROM model_run WHERE model_run_id=%s', (model_id,))
            row = q.fetchone()
        model.load_state_dict(trusted_load(row['checkpoint_path'], row['checkpoint_hash'], local))
        validation_path = local/f'epoch-{row["best_epoch"]:02}-validation.npy'
        data = np.load(validation_path, mmap_mode='r', allow_pickle=False)
        threshold = choose_threshold(data['score'], data['label'])
        values = evaluate_file(validation_path, threshold)
        atomic_json(local/'validation_metrics.json', values)
        atomic_json(local/'history.json', history)
        usage = resource_usage(device)
        record_metrics(lease.db, model_id, 'VALIDATION', values, sha(validation_path))
        outputs = [dict(path=str(p.relative_to(local)), sha256=sha(p), bytes=p.stat().st_size)
                   for p in sorted(local.iterdir()) if p.is_file() and p.name not in ('manifest.json', 'progress.json')]
        result = dict(status='SUCCEEDED', model_run_id=model_id, experiment_id=experiment, algorithm=algorithm,
                      config=identity, code_commit=commit(), environment_hash=sha(ROOT/'config/environment.lock.json'),
                      sample_hash=sample_hash, checkpoint_path=row['checkpoint_path'], checkpoint_hash=row['checkpoint_hash'],
                      best_epoch=row['best_epoch'], parameter_count=sum(p.numel() for p in model.parameters()),
                      threshold=threshold, validation_metrics=values, validation_scores=str(validation_path),
                      training_seconds=elapsed_before+time.monotonic()-started, hardware=hw, outputs=outputs, **usage)
        atomic_json(local/'manifest.json', result)
        with lease.db.cursor() as q:
            q.execute('UPDATE model_run SET status=%s,threshold=%s,training_seconds=%s,peak_allocated_mib=%s,peak_reserved_mib=%s,rss_mib=%s,updated_at=%s WHERE model_run_id=%s',
                      ('SUCCEEDED', threshold, result['training_seconds'], usage['peak_allocated_mib'], usage['peak_reserved_mib'], usage['rss_mib'], now(), model_id))
        atomic_json(local/'progress.json', dict(status='SUCCEEDED', best_epoch=row['best_epoch'], updated_at=stamp()))
        return result
    except BaseException:
        with lease.db.cursor() as q:
            q.execute('UPDATE model_run SET status=%s,updated_at=%s WHERE model_run_id=%s', ('FAILED', now(), model_id))
        raise
    finally:
        del model, optimizer
        if device.type == 'cuda':
            torch.cuda.empty_cache()
