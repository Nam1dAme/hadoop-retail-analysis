"""Bounded batch preparation and asynchronous CUDA copies."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import numpy as np
import torch

from .models import tensor_columns, tensors


def prefetch(source, prepare=lambda item, slot: item):
    """Keep one prepared item ahead. Callers can close early to join the worker."""
    source = iter(source)
    end = object()

    def load(slot):
        item = next(source, end)
        return end if item is end else prepare(item, slot)

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='ml-prefetch') as worker:
        slot = 0
        future = worker.submit(load, slot)
        while (item := future.result()) is not end:
            slot = 1 - slot
            future = worker.submit(load, slot)
            yield item


class _CudaStaging:
    def __init__(self, device, algorithm):
        self.device = device
        self.algorithm = algorithm
        self.stream = torch.cuda.Stream(device=device)
        self.slots = [{}, {}]
        self.ready = [None, None]

    def prepare(self, item, slot):
        batches, metadata = item
        ready = self.ready[slot]
        if ready is None:
            ready = self.ready[slot] = torch.cuda.Event()
        else:
            # The CPU must not overwrite pinned storage while DMA is reading it.
            ready.synchronize()
        inputs = []
        buffers = self.slots[slot]
        with torch.cuda.stream(self.stream):
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
                    fields[key] = host.to(self.device, non_blocking=True)
                inputs.append(fields)
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
