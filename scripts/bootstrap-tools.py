#!/usr/bin/env python3
"""只在var内安装锁定工具；不覆盖系统Java/Hadoop和已有非空安装。"""
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT=Path(__file__).resolve().parents[1]
lock=json.loads((ROOT/'config/toolchain.lock.json').read_text())
for name in ['java','hadoop']:
    spec=lock[name];target=ROOT/'var/tools'/spec['directory']
    if target.exists():
        print(f'{name}: 已存在，使用check-environment及版本锁核验');continue
    downloads=ROOT/'var/downloads';downloads.mkdir(parents=True,exist_ok=True)
    archive=downloads/f'{name}-locked.tar.gz'
    if not archive.exists():
        partial=archive.with_suffix('.partial')
        subprocess.run(['curl','--fail','--location','--retry','2','--output',str(partial),spec['url']],check=True)
        partial.rename(archive)
    algo='sha256' if name=='java' else 'sha512'
    digest=hashlib.new(algo)
    with archive.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
    if digest.hexdigest()!=spec[algo]:raise SystemExit(f'{name}: checksum mismatch')
    target.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as work:
        with tarfile.open(archive) as tar:tar.extractall(work,filter='data')
        children=list(Path(work).iterdir())
        if len(children)!=1 or not children[0].is_dir():raise SystemExit('Unexpected archive structure')
        children[0].rename(target)
    print(f'{name}: installed and checksum verified')
