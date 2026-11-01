#!/usr/bin/env python3
"""保存已通过探针的本机运行锁，不包含口令或数据内容。"""
from pathlib import Path
import hashlib,json,subprocess
ROOT=Path(__file__).resolve().parents[1]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
report=json.loads((ROOT/'var/environment-probe.json').read_text())
assert report['status']=='PASS'
source=ROOT/'var/source-sha256.txt'
assert source.exists() and len(source.read_text().split()[0])==64
host=json.loads((ROOT/'var/host-storage.json').read_text(encoding='utf-8-sig'))
assert any(x['distribution']=='Ubuntu-24.04' and x['gate_passed'] for x in host)
probe_files=sorted((ROOT/'var/probes').glob('*/evidence.json'))
assert probe_files
probe=json.loads(probe_files[-1].read_text());assert probe['status']=='PASS'
config_hashes={str(p.relative_to(ROOT)):sha(p) for p in sorted((ROOT/'config').rglob('*')) if p.is_file() and p.name!='environment.lock.json'}
lock=dict(report,uv_lock_sha256=sha(ROOT/'uv.lock'),toolchain_lock_sha256=sha(ROOT/'config/toolchain.lock.json'),
    config_hashes=config_hashes,source={'path':'/mnt/e/UserBehavior/UserBehavior.csv','bytes':Path('/mnt/e/UserBehavior/UserBehavior.csv').stat().st_size,'sha256':source.read_text().split()[0]},
    host_storage=host,hadoop_probe=probe,freeze_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip())
(ROOT/'config/environment.lock.json').write_text(json.dumps(lock,ensure_ascii=False,indent=2)+'\n')
print('已写入config/environment.lock.json（无凭据）')
