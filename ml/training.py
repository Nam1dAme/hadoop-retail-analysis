"""Full-population training, bounded scoring and epoch-boundary crash recovery."""
from contextlib import closing
import hashlib
import json
import math
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
from .models import make_model, seed_all, rule_scores, FEATURES, NUMERIC_DIM, GRU_LAYERS, GRU_HIDDEN
from . import gbdt as gbdt_module
from .pipeline import prepared_batches
from .shards import SPECS, training_groups
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
            'cpu_threads': torch.get_num_threads(),
            'tf32': bool(device.type == 'cuda' and torch.backends.cuda.matmul.allow_tf32),
            'deterministic': bool(torch.are_deterministic_algorithms_enabled()),
            'total_gpu_mib': torch.cuda.get_device_properties(0).total_memory / 1024 ** 2 if device.type == 'cuda' else None}


def amp_config(device):
    """Return (amp_enabled, amp_dtype, use_scaler, precision_name) for device."""
    if device.type != 'cuda':
        return False, torch.float32, False, 'FP32'
    if torch.cuda.is_bf16_supported():
        return True, torch.bfloat16, False, 'BF16_AMP'
    return True, torch.float16, True, 'FP16_AMP'


def scaled_learning_rate(base_lr, effective_batch):
    """Square-root learning-rate scaling relative to the 512 baseline batch size."""
    return float(base_lr) * math.sqrt(effective_batch / 512.0)


def compute_warmup_steps(total_rows, effective_batch):
    """Linear warmup over ~20% of epoch 1 when effective_batch > 512."""
    if effective_batch <= 512 or total_rows <= 0:
        return 0
    steps_per_epoch = math.ceil(total_rows / effective_batch)
    return max(1, math.ceil(0.2 * steps_per_epoch))


def make_warmup_scheduler(optimizer, warmup_steps):
    if warmup_steps <= 0:
        return torch.optim.lr_scheduler.LambdaLR(optimizer, lambda _step: 1.0)
    return torch.optim.lr_scheduler.LambdaLR(
        optimizer, lambda step: min(1.0, float(step + 1) / float(warmup_steps))
    )


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


def score(dataset, model, device, algorithm, output, batch_size=None, rate=None, lease=None, rule=None, gbdt=None,
          *, prefetch_batches=None):
    """Write exactly one score per sample in immutable input order; evaluation is FP32."""
    output = Path(output)
    temporary = output.with_suffix('.tmp.npy')
    prediction = np.lib.format.open_memmap(temporary, mode='w+', dtype=SCORE_DTYPE,
                                         shape=(dataset.manifest['rows'],))
    features = ('numeric',) if algorithm in ('LOGISTIC', 'RULE', 'GBDT') else FEATURES if algorithm == 'GRU' else ()
    columns = (*features, 'label', 'cohort', 'sample_key', 'as_of')
    if batch_size is None:
        batch_size = {'LOGISTIC': 4096, 'GRU': 2048}.get(algorithm, 512)
    if prefetch_batches is None:
        prefetch_batches = device.type == 'cuda'
    options = {'trim_sequences': True} if algorithm == 'GRU' else {}
    batches = dataset.batches(batch_size, shuffle=False, columns=columns, **options)
    if model is not None:
        model.eval()
        source = (([{key: batch[key] for key in features}], batch) for batch in batches)
        batches = prepared_batches(source, device, algorithm, prefetch_batches=prefetch_batches)
    else:
        batches = (([], batch) for batch in batches)
    offset = 0
    digest = hashlib.sha256()
    if device.type == 'cuda':
        prev_matmul_tf32 = torch.backends.cuda.matmul.allow_tf32
        prev_cudnn_tf32 = torch.backends.cudnn.allow_tf32
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.backends.cudnn.allow_tf32 = False
    try:
        with torch.inference_mode(), torch.autocast(device.type, enabled=False), closing(batches):
            for inputs, batch in batches:
                resource_guard(device, lease)
                if algorithm == 'CONSTANT':
                    values = np.full(len(batch['label']), rate, dtype=np.float64)
                elif algorithm == 'RULE':
                    if rule is None:
                        raise ManifestError('RULE scoring requires the frozen rule table')
                    values = rule_scores(batch['numeric'], rule)
                elif algorithm == 'GBDT':
                    if gbdt is None:
                        raise ManifestError('GBDT scoring requires the frozen model')
                    values = gbdt_module.predict_proba(gbdt, batch['numeric'])
                else:
                    values = torch.sigmoid(model(inputs[0]).float()).cpu().numpy()
                if not np.isfinite(values).all():
                    raise PipelineError('Non-finite model scores')
                end = offset + len(values)
                prediction['score'][offset:end] = values
                for key in ('label', 'cohort', 'sample_key', 'as_of'):
                    prediction[key][offset:end] = batch[key]
                for key in batch['sample_key']:
                    digest.update(bytes(key) + b'\n')
                offset = end
    finally:
        if device.type == 'cuda':
            torch.backends.cuda.matmul.allow_tf32 = prev_matmul_tf32
            torch.backends.cudnn.allow_tf32 = prev_cudnn_tf32
    if offset != dataset.manifest['rows'] or digest.hexdigest() != dataset.manifest['sample_order_sha256']:
        raise ManifestError('Prediction population/order changed')
    prediction.flush()
    del prediction
    os.replace(temporary, output)
    return {'path': str(output), 'sha256': sha(output), 'rows': offset, 'sample_order_sha256': digest.hexdigest()}


def record_metrics(db, model_id, split, result, evaluation_hash):
    with db.cursor() as q:
        names = ('ap', 'roc_auc', 'logloss', 'brier', 'precision', 'recall', 'f1', 'tp', 'fp', 'fn', 'tn',
                 *(f'{kind}_at_{suffix}' for kind in ('precision', 'recall', 'lift') for suffix in ('1pct', '5pct', '10pct')))
        if max(len(name) for name in names) > 32 or any(len(cohort) > 32 for cohort in result):
            raise ManifestError('Metric dimension exceeds VARCHAR(32)')
        for cohort, values in result.items():
            for name in names:
                q.execute('INSERT INTO model_metric VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value),sample_count=VALUES(sample_count),positive_count=VALUES(positive_count),reason=VALUES(reason),evaluation_hash=VALUES(evaluation_hash)',
                          (model_id, split, cohort, name, values.get(name), values['sample_count'], values['positive_count'], values.get('reason'), evaluation_hash))


def backward_group(model, scaler, group, weights, device, *, amp_dtype=None):
    loss_total = None
    if amp_dtype is None:
        amp_enabled, amp_dtype, _, _ = amp_config(device)
    else:
        amp_enabled = device.type == 'cuda' and amp_dtype in (torch.float16, torch.bfloat16)
    for inputs, weight in zip(group, weights):
        with torch.autocast(device.type, dtype=amp_dtype, enabled=amp_enabled):
            logits = model(inputs)
        loss = F.binary_cross_entropy_with_logits(logits.float(), inputs['label'])
        weighted = loss.detach().double() * len(inputs['label'])
        loss_total = weighted if loss_total is None else loss_total + weighted
        scaler.scale(loss * weight).backward()
    value = loss_total.item()
    if not math.isfinite(value):
        raise PipelineError('Non-finite training loss')
    return value


def optimizer_update(model, optimizer, scaler):
    scaler.unscale_(optimizer)
    norm = torch.nn.utils.clip_grad_norm_(model.parameters(), 1.)
    previous_scale = scaler.get_scale()
    if not scaler.is_enabled() and not torch.isfinite(norm):
        raise PipelineError('Non-finite unscaled gradient')
    scaler.step(optimizer)
    scaler.update()
    skipped = scaler.get_scale() < previous_scale
    if scaler.is_enabled() and not skipped and not torch.isfinite(norm):
        raise PipelineError('Non-finite unscaled gradient')
    return skipped


def train_batches(train, device, algorithm, micro, effective, seed, epoch, *, bucket_by_length, prefetch_batches):
    columns = (*FEATURES, 'label') if algorithm == 'GRU' else ('numeric', 'label')
    groups = training_groups(train, micro, effective, seed=seed, epoch=epoch, columns=columns,
                             bucket_by_length=bucket_by_length, trim_sequences=algorithm == 'GRU')
    source = ((group, (weights, count)) for group, weights, count in groups)
    return prepared_batches(source, device, algorithm, prefetch_batches=prefetch_batches)


def _probe_batch(train, algorithm, device, seed, micro):
    seed_all(seed)
    model = make_model(algorithm).to(device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=.001, weight_decay=1e-4)
    amp_enabled, amp_dtype, use_scaler, precision_label = amp_config(device)
    scaler = torch.amp.GradScaler('cuda', enabled=use_scaler)
    columns = (*FEATURES, 'label') if algorithm == 'GRU' else ('numeric', 'label')
    rows = min(micro, max(shard['rows'] for shard in train.manifest['shards']))
    # Reserve for the largest packed shape and both staging slots, including the long-tail bucket.
    batch = {key: np.zeros((rows, *SPECS[key][1]), dtype=SPECS[key][0]) for key in columns}
    if algorithm == 'GRU':
        batch['length'].fill(SPECS['category'][1][0])
        for key in ('category', 'behavior', 'hour'):
            batch[key].fill(1)
    batch['label'][::2] = 1
    precision = {}

    def record(name, select):
        def observe(module, inputs, output):
            precision[name] = str(select(output).dtype)
        return observe

    model.register_forward_hook(record('logits_dtype', lambda output: output))
    if algorithm == 'GRU':
        model.gru.register_forward_hook(record('gru_dtype', lambda output: output[1]))
    skipped = 0
    steps = 5
    batches = prepared_batches((([batch], None) for _ in range(steps)), device, algorithm, prefetch_batches=amp_enabled)
    with closing(batches):
        for inputs, _ in batches:
            optimizer.zero_grad(set_to_none=True)
            backward_group(model, scaler, inputs, [1.], device, amp_dtype=amp_dtype)
            skipped += optimizer_update(model, optimizer, scaler)
    if skipped == steps or (amp_enabled and precision['logits_dtype'] != str(amp_dtype)):
        raise PipelineError('Preflight did not observe a finite optimizer update with the requested precision')
    return dict(tested_rows=rows, amp=amp_enabled, precision=precision_label,
                optimizer_updates=steps - skipped, skipped_updates=skipped,
                **precision, **resource_usage(device))


def preflight(train, algorithm, device, seed=42):
    if algorithm == 'GRU' and device.type != 'cuda':
        raise PipelineError('Formal GRU training requires CUDA')
    if device.type == 'cuda' and not torch.cuda.is_available():
        raise PipelineError('CUDA is unavailable for training preflight')
    if not train.manifest['rows']:
        raise PipelineError('Training requires a nonempty TRAIN split')
    candidates = (4096, 2048, 1024, 512, 256, 128, 64, 32) if device.type == 'cuda' else (512,)
    evidence = []
    for micro in candidates:
        try:
            if device.type == 'cuda':
                torch.cuda.empty_cache()
                torch.cuda.reset_peak_memory_stats()
            usage = _probe_batch(train, algorithm, device, seed, micro)
            evidence.append(dict(micro_batch=micro, **usage))
            if usage['peak_allocated_mib'] <= 6144 and usage['peak_reserved_mib'] <= 6656 and usage['rss_mib'] <= 3072:
                return micro, evidence
        except torch.cuda.OutOfMemoryError:
            evidence.append(dict(micro_batch=micro, error='CUDA_OOM'))
        finally:
            if device.type == 'cuda':
                torch.cuda.empty_cache()
    raise PipelineError('Training preflight failed at minimum batch: ' + encode(evidence))


def fit(lease, experiment, train, validation, algorithm, learning_rate, directory, sample_hash, seed=42, *, device=None):
    device = torch.device(device or ('cuda' if (algorithm == 'GRU' or torch.cuda.is_available()) else 'cpu'))
    micro, preflight_evidence = preflight(train, algorithm, device, seed)
    effective_batch = 4096 if (algorithm == 'GRU' or device.type == 'cuda') else 512
    bucket_by_length = algorithm == 'GRU'
    prefetch_batches = device.type == 'cuda'
    amp_enabled, amp_dtype, use_scaler, precision_label = amp_config(device)
    base_lr = float(learning_rate)
    lr_scaled = scaled_learning_rate(base_lr, effective_batch)
    warmup_steps = compute_warmup_steps(train.manifest['rows'], effective_batch)
    identity = dict(schema_version='model-v2', algorithm=algorithm, learning_rate=base_lr,
                    base_lr=base_lr, scaled_lr=lr_scaled, warmup_steps=warmup_steps,
                    numeric_dim=NUMERIC_DIM, weight_decay=1e-4, seed=seed, effective_batch=effective_batch, micro_batch=micro,
                    device=str(device), precision=precision_label,
                    bucket_by_length=bucket_by_length, prefetch_batches=prefetch_batches,
                    max_epochs=20, patience=3, selection='VALIDATION_AP', sample_hash=sample_hash,
                    implementation=fingerprint({str(p.relative_to(ROOT)): sha(p) for p in sorted((ROOT/'ml').glob('*.py'))
                                                if p.name not in ('benchmark.py', 'profile_pipeline.py')}))
    if algorithm == 'GRU':
        identity['gru_layers'] = GRU_LAYERS
        identity['gru_hidden'] = GRU_HIDDEN
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
        seed_all(seed)
        model = make_model(algorithm).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=lr_scaled, weight_decay=1e-4)
        scheduler = make_warmup_scheduler(optimizer, warmup_steps)
        scaler = torch.amp.GradScaler('cuda', enabled=use_scaler)
        hw = hardware(device)
        if row:
            q.execute('UPDATE model_run SET status=%s,updated_at=%s WHERE model_run_id=%s', ('RUNNING', now(), model_id))
        else:
            q.execute('INSERT INTO model_run(model_run_id,experiment_id,algorithm,status,seed,config_hash,config_json,device,parameter_count,hardware_json,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                      (model_id, experiment, algorithm, 'RUNNING', seed, config_hash, encode(identity), str(device), sum(p.numel() for p in model.parameters()), encode(hw), now(), now()))
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
        if 'scheduler' in checkpoint:
            scheduler.load_state_dict(checkpoint['scheduler'])
        scaler.load_state_dict(checkpoint['scaler'])
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
        for epoch in range(first_epoch, identity['max_epochs'] + 1):
            if stale >= identity['patience']:
                break
            epoch_started = time.monotonic()
            model.train()
            total = 0
            loss_total = 0.
            skipped = 0
            batches = train_batches(train, device, algorithm, micro, effective_batch, seed, epoch,
                                    bucket_by_length=bucket_by_length, prefetch_batches=prefetch_batches)
            last_guard = 0.0
            with closing(batches):
                for index, (group, (weights, count)) in enumerate(batches):
                    now_mono = time.monotonic()
                    if now_mono - last_guard >= 5.0:
                        resource_guard(device, lease)
                        last_guard = now_mono
                    optimizer.zero_grad(set_to_none=True)
                    loss_total += backward_group(model, scaler, group, weights, device, amp_dtype=amp_dtype)
                    step_skipped = optimizer_update(model, optimizer, scaler)
                    skipped += step_skipped
                    if not step_skipped:
                        scheduler.step()
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
                         scheduler=scheduler.state_dict(), scaler=scaler.state_dict(), torch_rng=torch.get_rng_state(),
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
            # Clean up superseded epoch checkpoints only after the DB transaction commits.
            for old in local.glob('epoch-*-resume.pt'):
                if old != resume_path:
                    old.unlink(missing_ok=True)
            if improved:
                val_path = local/f'epoch-{epoch:02}-validation.npy'
                for old in local.glob('epoch-*-state.pt'):
                    if old != best_path:
                        old.unlink(missing_ok=True)
                for old in local.glob('epoch-*-validation.npy'):
                    if old != val_path:
                        old.unlink(missing_ok=True)
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
        for old in local.glob('epoch-*-resume.pt'):
            old.unlink(missing_ok=True)
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
            q.execute('UPDATE model_run SET status=%s,resume_path=NULL,resume_hash=NULL,threshold=%s,training_seconds=%s,peak_allocated_mib=%s,peak_reserved_mib=%s,rss_mib=%s,updated_at=%s WHERE model_run_id=%s',
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
