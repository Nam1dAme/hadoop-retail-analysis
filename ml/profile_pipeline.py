"""Offline synthetic throughput probe. Never imports an experiment or opens a service.

Run with the worktree Python: ``python -m ml.profile_pipeline --help``.
All samples, checkpoints and machine-readable results belong under /tmp.
"""
import argparse
from contextlib import closing, contextmanager
import json
from pathlib import Path
import subprocess
import threading
import time

import numpy as np
import torch

from .models import FEATURES, NUMERIC_DIM, make_model, seed_all, tensors
from .pipeline import prefetch, prepared_batches
from .shards import ShardDataset, ShardWriter, training_groups
from .training import backward_group, optimizer_update, resource_usage, score


def generate(path, rows, seed):
    rng = np.random.default_rng(seed)
    writer = ShardWriter(path, np.zeros(NUMERIC_DIM), np.ones(NUMERIC_DIM))
    for i in range(rows):
        length = min(int(rng.geometric(.12)), 128)
        if i % 100 == 0:
            length = 128  # A 1% long tail; median 6, mean about 9.5.
        category = np.zeros(128, dtype=np.uint16)
        behavior = np.zeros(128, dtype=np.uint8)
        hour = np.zeros(128, dtype=np.uint8)
        gap = np.zeros(128, dtype=np.float32)
        category[:length] = rng.integers(1, 16385, length)
        behavior[:length] = rng.integers(1, 5, length)
        hour[:length] = rng.integers(1, 25, length)
        gap[1:length] = rng.random(length - 1)
        writer.add(dict(user_id=str(i + 1), as_of=1512057600, length=length,
                        category=category, behavior=behavior, hour=hour, gap=gap,
                        numeric=rng.normal(size=NUMERIC_DIM), label=i % 5 == 0,
                        history_event_count=length, cohort='SEEN_TRAIN' if i % 3 else 'UNSEEN_TRAIN'))
        if (i + 1) % 100000 == 0:
            print(json.dumps(dict(generated=i + 1)), flush=True)
    return writer.finish()


@contextmanager
def gpu_samples():
    result = dict(samples=[], error=None)
    stopped = threading.Event()

    def collect():
        while not stopped.is_set():
            try:
                proc = subprocess.run(['nvidia-smi', '--query-gpu=utilization.gpu,memory.used',
                                       '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=5)
                if proc.returncode:
                    result['error'] = proc.stderr.strip() or proc.stdout.strip()
                    return
                util, memory = map(float, proc.stdout.strip().splitlines()[0].split(','))
                result['samples'].append(dict(time=time.monotonic(), utilization=util, memory_mib=memory))
            except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
                result['error'] = str(exc)
                return
            stopped.wait(.5)

    thread = threading.Thread(target=collect, daemon=True)
    thread.start()
    try:
        yield result
    finally:
        stopped.set()
        thread.join()


def timed_batches(iterator, timings):
    iterator = iter(iterator)
    while True:
        started = time.perf_counter()
        try:
            value = next(iterator)
        except StopIteration:
            timings['read_batch'] += time.perf_counter() - started
            return
        timings['read_batch'] += time.perf_counter() - started
        yield value


def timed_cuda_inputs(group, device, algorithm, copy_start, copy_end):
    host = [{key: value if key == 'length' else value.pin_memory()
             for key, value in tensors(batch, torch.device('cpu'), algorithm).items()}
            for batch in group]
    copy_start.record()
    inputs = [{key: value if key == 'length' else value.to(device, non_blocking=True)
               for key, value in batch.items()} for batch in host]
    copy_end.record()
    copy_end.synchronize()
    return inputs, copy_start.elapsed_time(copy_end) / 1000


def probe(args, dataset, device):
    timings = dict(read_batch=0., mmap_open=0., tensor_h2d=None if args.prefetch else 0.,
                   h2d_window=None, prepared_wait=0., forward_backward=0., optimizer=0.)
    original_load = np.load

    def measured_load(*a, **kw):
        started = time.perf_counter()
        result = original_load(*a, **kw)
        timings['mmap_open'] += time.perf_counter() - started
        return result

    def synchronize():
        if device.type == 'cuda':
            torch.cuda.synchronize()

    columns = (*FEATURES, 'label') if args.algorithm == 'GRU' else ('numeric', 'label')
    options = dict(seed=args.seed, epoch=1, columns=columns, bucket_by_length=args.bucket, trim_sequences=args.trim)
    seed_all(args.seed)
    if args.mode != 'data':
        model = make_model(args.algorithm).to(device)
    if args.mode == 'train':
        optimizer = torch.optim.AdamW(model.parameters(), lr=.001, weight_decay=1e-4)
        scaler = torch.amp.GradScaler('cuda', enabled=device.type == 'cuda' and not args.fp32)
    measure_copy = args.mode == 'train' and args.detail and not args.prefetch and device.type == 'cuda'
    if measure_copy:
        events = (torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True))
        timings['h2d_window'] = 0.
    count = 0
    padded_steps = 0
    np.load = measured_load
    synchronize()
    if device.type == 'cuda':
        torch.cuda.reset_peak_memory_stats()
    started = time.perf_counter()
    try:
        if args.mode == 'data':
            iterator = timed_batches(dataset.batches(args.batch, **options), timings)
            if args.prefetch:
                iterator = prefetch(iterator)
            with closing(iterator):
                for batch in iterator:
                    n = len(batch['label'])
                    count += n
                    if args.algorithm == 'GRU':
                        padded_steps += n * int(batch['length'].max())
        elif args.mode == 'score':
            score(dataset, model, device, args.algorithm, args.output.with_suffix('.scores.npy'),
                  batch_size=args.batch, prefetch_batches=args.prefetch)
            count = dataset.manifest['rows']
        else:
            raw = training_groups(dataset, args.batch, args.effective, **options)
            iterator = timed_batches(raw, timings)
            if args.prefetch:
                source = ((group, (weights, n)) for group, weights, n in iterator)
                iterator = prepared_batches(source, device, args.algorithm, prefetch_batches=True)
            with closing(iterator):
                while not args.limit or count < args.limit:
                    tick = time.perf_counter()
                    item = next(iterator, None)
                    if item is None:
                        break
                    if args.prefetch:
                        inputs, (weights, n) = item
                        timings['prepared_wait'] += time.perf_counter() - tick
                    else:
                        group, weights, n = item
                        tick = time.perf_counter()
                        if measure_copy:
                            inputs, seconds = timed_cuda_inputs(group, device, args.algorithm, *events)
                            timings['h2d_window'] += seconds
                        else:
                            inputs = [tensors(batch, device, args.algorithm) for batch in group]
                        timings['tensor_h2d'] += time.perf_counter() - tick
                    optimizer.zero_grad(set_to_none=True)
                    tick = time.perf_counter()
                    backward_group(model, scaler, inputs, weights, device)
                    timings['forward_backward'] += time.perf_counter() - tick
                    tick = time.perf_counter()
                    optimizer_update(model, optimizer, scaler)
                    if args.detail:
                        synchronize()
                    timings['optimizer'] += time.perf_counter() - tick
                    count += n
    finally:
        synchronize()
        elapsed = time.perf_counter() - started
        np.load = original_load
    timings['batch_materialize'] = max(0., timings['read_batch'] - timings['mmap_open']) if args.mode != 'score' else None
    return dict(rows=count, seconds=elapsed, samples_per_second=count / elapsed,
                extrapolated_3460000_seconds=3460000 * elapsed / count,
                padded_steps=padded_steps or None, timings=timings, **resource_usage(device))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=Path('/tmp/wt-gpu-throughput/TRAIN'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--generate', type=int, default=0, metavar='ROWS')
    parser.add_argument('--mode', choices=('data', 'train', 'score'), default='data')
    parser.add_argument('--algorithm', choices=('GRU', 'LOGISTIC'), default='GRU')
    parser.add_argument('--device', choices=('cpu', 'cuda'), default='cpu')
    parser.add_argument('--batch', type=int, default=256)
    parser.add_argument('--effective', type=int, default=512)
    parser.add_argument('--limit', type=int, default=32768, help='Training rows per repeat; 0 = all')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--prefetch', action='store_true')
    parser.add_argument('--bucket', action='store_true')
    parser.add_argument('--trim', action='store_true')
    parser.add_argument('--detail', action='store_true', help='Synchronize unprefetched CUDA phases for attribution')
    parser.add_argument('--fp32', action='store_true')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    for path in (args.data, args.output):
        if not path.resolve().is_relative_to('/tmp'):
            parser.error('Synthetic data and output must be under /tmp')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.generate:
        generate(args.data, args.generate, args.seed)
    dataset = ShardDataset(args.data, verify=False)
    device = torch.device(args.device)
    if device.type == 'cuda' and not torch.cuda.is_available():
        parser.error('CUDA unavailable; select cpu explicitly for CPU-only evidence')
    with gpu_samples() as gpu:
        records = []
        for repeat in range(args.repeats):
            result = probe(args, dataset, device)
            records.append(result)
            print(json.dumps(dict(repeat=repeat, **result)), flush=True)
    result = dict(config={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
                  torch=str(torch.__version__), cuda_available=torch.cuda.is_available(),
                  dataset_rows=dataset.manifest['rows'], records=records, gpu=gpu)
    args.output.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
