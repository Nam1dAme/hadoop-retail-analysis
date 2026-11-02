"""Comparable FP32 CPU/CUDA benchmark with deterministic sample-hash selection."""
import hashlib
import resource
import time
import uuid
from pathlib import Path
import numpy as np
import torch
from torch.nn import functional as F
from orchestration.common import atomic_json, fingerprint, encode
from .models import make_model, seed_all, tensors, FEATURES
from .training import hardware, resource_usage, resource_guard


class ComparableDropout(torch.nn.Module):
    """Use the same CPU-generated masks on both devices for the FP32 comparison."""
    def forward(self, values):
        if not self.training:
            return values
        mask = (torch.rand(values.shape, device='cpu') >= .1).to(values.device)
        return values * mask / .9


def select_sample(train, output, limit=65536):
    index = np.empty(0, dtype=[('key', 'S64'), ('shard', '<i4'), ('row', '<i4')])
    for number, shard in enumerate(train.manifest['shards']):
        p = train.path/shard['directory']
        keys = np.load(p/'sample_key.npy', mmap_mode='r', allow_pickle=False)
        added = np.empty(len(keys), dtype=index.dtype)
        added['key'], added['shard'], added['row'] = keys, number, np.arange(len(keys))
        combined = np.concatenate((index, added))
        index = combined[np.argsort(combined['key'], kind='stable')[:limit]]
    if len(index) < 4096:
        raise ValueError('Benchmark needs at least 4096 TRAIN samples')
    result = {}
    for key in (*FEATURES, 'label'):
        first = np.load(train.path/train.manifest['shards'][0]['directory']/(key+'.npy'), mmap_mode='r', allow_pickle=False)
        result[key] = np.empty((len(index), *first.shape[1:]), dtype=first.dtype)
    for number, shard in enumerate(train.manifest['shards']):
        mask = index['shard'] == number
        if not mask.any():
            continue
        for key in result:
            column = np.load(train.path/shard['directory']/(key+'.npy'), mmap_mode='r', allow_pickle=False)
            result[key][mask] = column[index['row'][mask]]
    digest = hashlib.sha256(b''.join(bytes(k)+b'\n' for k in index['key'])).hexdigest()
    atomic_json(output, {'rows': len(index), 'sample_hash': digest, 'selection': 'smallest TRAIN sample_key hashes'})
    return result, digest


def run_benchmark(lease, train, experiment, directory):
    path = Path(directory)/'benchmark.json'
    if path.exists():
        import json
        result = json.loads(path.read_text())
        if result['status'] == 'PASS' and result['train_manifest_hash'] == fingerprint(train.manifest):
            return result
    sample_started = time.monotonic()
    sample, digest = select_sample(train, Path(directory)/'benchmark-samples.json')
    preload_seconds = time.monotonic()-sample_started
    count = len(sample['label'])
    configuration = dict(batch=64, warmup_steps=20, steps=200, repeats=3, precision='FP32',
                         tf32=False, cpu_threads=4, dropout='same CPU RNG mask on both devices')
    records, first_losses = [], {}
    for name in ('cpu', 'cuda'):
        device = torch.device(name)
        if name == 'cuda' and not torch.cuda.is_available():
            raise RuntimeError('CUDA benchmark cannot be replaced by CPU')
        for repeat in range(3):
            resource_guard(device, lease)
            end_to_end = time.monotonic()
            seed_all()
            model = make_model('GRU').to(device)
            model.head[2] = ComparableDropout()
            optimizer = torch.optim.AdamW(model.parameters(), lr=.001, weight_decay=1e-4)

            def step(i):
                start = (i*64) % count
                # count is 65536 in production, or a complete prefix for smaller valid fixtures.
                idx = np.arange(start, start+64) % count
                batch = {k: v[idx].copy() for k, v in sample.items()}
                inputs = tensors(batch, device)
                optimizer.zero_grad(set_to_none=True)
                loss = F.binary_cross_entropy_with_logits(model(inputs).float(), inputs['label'])
                if not torch.isfinite(loss):
                    raise RuntimeError('Non-finite FP32 benchmark')
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), 1., error_if_nonfinite=True)
                optimizer.step()
                return loss.detach().item()

            for i in range(20):
                step(i)
            del optimizer, model
            seed_all()
            model = make_model('GRU').to(device)
            model.head[2] = ComparableDropout()
            optimizer = torch.optim.AdamW(model.parameters(), lr=.001, weight_decay=1e-4)
            old = model.head[-1].weight.detach().clone()
            if name == 'cuda':
                torch.cuda.synchronize()
                torch.cuda.reset_peak_memory_stats()
            started = time.monotonic()
            first = None
            for i in range(200):
                loss = step(i)
                if first is None:
                    first = loss
                if i % 20 == 0:
                    resource_guard(device, lease)
            if name == 'cuda':
                torch.cuda.synchronize()
            elapsed = time.monotonic()-started
            assert not torch.equal(old, model.head[-1].weight), 'Benchmark must update parameters'
            usage = resource_usage(device)
            record = dict(benchmark_id=str(uuid.uuid4()), device=name, repeat_index=repeat,
                          elapsed_seconds=elapsed, end_to_end_seconds=time.monotonic()-end_to_end,
                          throughput=200*64/elapsed, steps=200, processed_samples=200*64,
                          first_loss=first, hardware=hardware(device), **usage)
            records.append(record)
            first_losses.setdefault(name, []).append(first)
            print(encode(dict(benchmark=name, repeat=repeat, seconds=elapsed)), flush=True)
            with lease.db.cursor() as q:
                q.execute('INSERT INTO benchmark VALUES(%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                          (record['benchmark_id'], experiment, name, 'FP32', repeat, digest, fingerprint(configuration),
                           200, elapsed, record['end_to_end_seconds'], record['throughput'],
                           usage['peak_allocated_mib'], usage['peak_reserved_mib'], usage['rss_mib'], encode(record['hardware'])))
            del model, optimizer, old
            if name == 'cuda':
                torch.cuda.empty_cache()
    differences = [abs(c-g) for c, g in zip(first_losses['cpu'], first_losses['cuda'])]
    if max(differences) > 1e-4:
        raise RuntimeError('CPU/CUDA first-step loss differs beyond tolerance')
    medians = {name: float(np.median([r['elapsed_seconds'] for r in records if r['device'] == name])) for name in ('cpu', 'cuda')}
    result = dict(status='PASS', config=configuration, sample_hash=digest, rows=count,
                  train_manifest_hash=fingerprint(train.manifest), preload_seconds=preload_seconds,
                  records=records, first_loss_differences=differences, median_seconds=medians,
                  speedup=medians['cpu']/medians['cuda'])
    atomic_json(path, result)
    return result
