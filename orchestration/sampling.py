"""Deterministic exact bottom-k record samples; only for throughput, never model labels."""
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import numpy as np
from .common import atomic_json,sha,ManifestError,stamp

RECORD=np.dtype([('hi','>u8'),('lo','>u8'),('offset','<u8')])


def record_samples(source,directory,expected_sha,expected_rows,sizes=(1_000_000,10_000_000),guard=None):
    source=Path(source);directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    if not sizes or min(sizes)<1 or max(sizes)>expected_rows:raise ValueError('Invalid sample sizes')
    manifest=directory/'sampling.json'
    if manifest.exists():
        previous=json.loads(manifest.read_text())
        if previous['source_sha256']!=expected_sha or sha(source)!=expected_sha:raise ManifestError('Sampling source changed')
        if set(previous['samples'])!={str(n) for n in sizes}:raise ManifestError('Sampling sizes changed')
        for item in previous['samples'].values():
            if sha(item['path'])!=item['sha256']:raise ManifestError('Deterministic sample changed')
        return previous
    before=source.stat();prefix=min(256,math.ceil(256*max(sizes)/expected_rows*1.3))
    filtered=directory/'hash-prefix.tmp.bin'
    while True:
        digest=hashlib.sha256();offset=rows=retained=0
        with source.open('rb',buffering=8*1024**2) as inp,filtered.open('wb',buffering=8*1024**2) as out:
            for line in inp:
                digest.update(line)
                key=hashlib.sha256(offset.to_bytes(8,'big')+line).digest()[:16]
                if key[0]<prefix:out.write(key+struct.pack('<Q',offset));retained+=1
                offset+=len(line);rows+=1
                if guard and rows%100000==0:guard()
        if digest.hexdigest()!=expected_sha or rows!=expected_rows:raise ManifestError('Sampling input differs from full verified source')
        if retained>=max(sizes):break
        prefix=min(256,prefix*2)
    records=np.fromfile(filtered,dtype=RECORD)
    if guard:guard()
    order=np.argsort(records,order=('hi','lo','offset'),kind='stable')
    offsets={n:np.sort(records['offset'][order[:n]]) for n in sizes}
    del records,order
    paths={n:directory/f'records-{n}.csv' for n in sizes}
    if any(p.exists() for p in paths.values()):raise ManifestError('Refuse overwriting unregistered samples')
    handles={n:p.with_suffix('.csv.tmp').open('xb',buffering=8*1024**2) for n,p in paths.items()}
    counts={n:0 for n in sizes};hashes={n:hashlib.sha256() for n in sizes};offset=0
    next_offsets={n:int(offsets[n][0]) for n in sizes}
    try:
        with source.open('rb',buffering=8*1024**2) as inp:
            for row_number,line in enumerate(inp):
                if guard and row_number%100000==0:guard()
                for n in sizes:
                    i=counts[n]
                    if i<n and offset==next_offsets[n]:
                        handles[n].write(line);hashes[n].update(line);counts[n]+=1
                        if counts[n]<n:next_offsets[n]=int(offsets[n][counts[n]])
                offset+=len(line)
        after=source.stat()
        if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise ManifestError('Source changed during sampling')
        assert all(counts[n]==n for n in sizes)
        for n,f in handles.items():f.flush();os.fsync(f.fileno());f.close();os.replace(paths[n].with_suffix('.csv.tmp'),paths[n])
    finally:
        for f in handles.values():f.close()
    result=dict(status='SUCCEEDED',sampled=True,model_training_allowed=False,source_path=str(source),source_sha256=expected_sha,
                source_rows=expected_rows,method='bottom-k SHA256(source-byte-offset-big-endian || raw-record)[:16], offset tie-break',
                observation='Record sampling breaks user history; these files are throughput fixtures only.',
                created_at=stamp(),samples={str(n):dict(path=str(paths[n]),rows=n,bytes=paths[n].stat().st_size,sha256=hashes[n].hexdigest()) for n in sizes})
    atomic_json(manifest,result);filtered.unlink()
    return result
