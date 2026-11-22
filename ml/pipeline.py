"""Bounded batch preparation and asynchronous CUDA copies."""
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing
import threading

import numpy as np
import torch

from .models import tensor_columns, tensors

PREFETCH_WORKERS = 3
PREFETCH_BUFFER = 8


def prefetch(source, prepare=lambda item, slot: item, *, max_workers=PREFETCH_WORKERS, buffer_size=PREFETCH_BUFFER):
    """Keep up to buffer_size prepared items ahead while preserving exact source order."""
    if max_workers < 1 or buffer_size < 1:
        raise ValueError('Invalid prefetch concurrency')
    source = iter(source)
    end = object()
    lock = threading.Lock()
    exhausted = False
    next_seq = 0

    def load():
        nonlocal exhausted, next_seq
        with lock:
            if exhausted:
                return None, end
            item = next(source, end)
            if item is end:
                exhausted = True
                return None, end
            seq = next_seq
            next_seq += 1
        return seq, prepare(item, seq % buffer_size)

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='ml-prefetch') as worker:
        in_flight = set()
        pending = {}
        expected_seq = 0
        try:
            for _ in range(buffer_size):
                in_flight.add(worker.submit(load))
            while in_flight or expected_seq in pending:
                while expected_seq not in pending and in_flight:
                    done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                    for future in done:
                        seq, prepared = future.result()
                        if seq is not None:
                            pending[seq] = prepared
                    while not exhausted and len(in_flight) + len(pending) < buffer_size:
                        in_flight.add(worker.submit(load))
                if expected_seq not in pending:
                    break
                yield pending.pop(expected_seq)
                expected_seq += 1
                while not exhausted and len(in_flight) + len(pending) < buffer_size:
                    in_flight.add(worker.submit(load))
        finally:
            for future in in_flight:
                future.cancel()


class _CudaStaging:
    def __init__(self, device, algorithm, num_slots=PREFETCH_BUFFER):
        self.device = device
        self.algorithm = algorithm
        self.stream = torch.cuda.Stream(device=device)
        self.stream_lock = threading.Lock()
        self.slots = [{} for _ in range(num_slots)]
        self.ready = [None] * num_slots

    def prepare(self, item, slot):
        batches, metadata = item
        ready = self.ready[slot]
        if ready is None:
            ready = self.ready[slot] = torch.cuda.Event()
        else:
            # The CPU must not overwrite pinned storage while DMA is reading it.
            ready.synchronize()
        buffers = self.slots[slot]
        host_batches = []
        for index, batch in enumerate(batches):
            fields = {}
            for key, array, dtype in tensor_columns(batch, self.algorithm):
                if key == 'length':
                    fields[key] = torch.from_numpy(array).to(dtype=dtype)
                    continue
                buffer = buffers.get((index, key))
                if buffer is None or buffer.numel() < array.size:
                    buffer = buffers[index, key] = torch.empty(array.size, dtype=dtype, pin_memory=True)
                host = buffer[:array.size].view(array.shape)
                np.copyto(host.numpy(), array)
                fields[key] = host
            host_batches.append(fields)
        inputs = []
        with self.stream_lock, torch.cuda.stream(self.stream):
            for fields in host_batches:
                inputs.append({key: (value if key == 'length' else value.to(self.device, non_blocking=True))
                               for key, value in fields.items()})
            ready.record(self.stream)
        return inputs, metadata, ready


def prepared_batches(source, device, algorithm, *, prefetch_batches):
    """Convert a sequence of (microbatches, metadata) into model inputs in order."""
    if not prefetch_batches or device.type != 'cuda':
        def prepare(item, slot):
            batches, metadata = item
            return [tensors(batch, device, algorithm) for batch in batches], metadata

        if prefetch_batches:
            with closing(prefetch(source, prepare)) as batches:
                yield from batches
        else:
            for item in source:
                yield prepare(item, 0)
        return

    staging = _CudaStaging(device, algorithm)
    compute = torch.cuda.current_stream(device)
    try:
        with closing(prefetch(source, staging.prepare)) as batches:
            for inputs, metadata, ready in batches:
                compute.wait_event(ready)
                # CUDA allocations originate on the copy stream and are consumed on compute.
                for batch in inputs:
                    for value in batch.values():
                        if value.is_cuda:
                            value.record_stream(compute)
                yield inputs, metadata
    finally:
        staging.stream.synchronize()
