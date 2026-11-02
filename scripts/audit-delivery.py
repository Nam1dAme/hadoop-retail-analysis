#!/usr/bin/env python3
"""Audit evidence presence and repository integrity, without inventing test results."""
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote,urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,sha,stamp,commit

# A report's PASS is scoped to its own test, never a substitute for another gate.
reports={
 'G-DATA-MR':('var/production/m123-verification.json',),
 'G-MODEL-GPU':('var/production/model-verification.json','var/production/cuda-recovery-test.json'),
 'G-RELEASE':('var/production/release-verification.json',),
 'G-APP':('var/production/browser/verification.json','var/production/api-load-test.json','var/production/permission-verification.json'),
 'G-SCALE':('var/scale/verification.json','var/fixtures/hotspot-verification.json'),
 'G-LOCAL-RECOVERY':('var/production/backup.json','var/production/recovery.json'),
}
results=[]
for gate,paths in reports.items():
    evidence=[]
    for relative in paths:
        path=ROOT/relative
        if not path.exists():evidence.append(dict(path=relative,status='NOT_RUN'));continue
        value=json.loads(path.read_text())
        evidence.append(dict(path=relative,status=value.get('status','UNKNOWN'),sha256=sha(path)))
    results.append(dict(gate=gate,status='PASS' if all(x['status']=='PASS' for x in evidence) else 'INCOMPLETE',evidence=evidence))
# Physical independence cannot be inferred from a successful local restore.
backup=ROOT/'var/production/backup.json';recovery=ROOT/'var/production/recovery.json'
independent=all(p.exists() and json.loads(p.read_text()).get('independent_backup_verified') is True for p in (backup,recovery))
results.append(dict(gate='G-INDEPENDENT-BACKUP',status='PASS' if independent else 'NOT_VERIFIED',reason='User confirmed no separate physical disk; local copies do not prove disk-failure recovery.'))
frozen=[]
for name,digest in re.findall(r'\[([^\]]+\.md)\]\([^)]*\) \| `([0-9a-f]{64})`',(ROOT/'docs/BASELINE-MANIFEST.md').read_text()):
    path=ROOT/'docs'/name;actual=sha(path) if path.is_file() else None
    frozen.append(dict(path=str(path.relative_to(ROOT)),expected_sha256=digest,actual_sha256=actual,status='PASS' if actual==digest else 'FAIL'))
tracked=subprocess.check_output(['git','ls-files','-z'],cwd=ROOT).decode().split('\0');files=[];broken=[];forbidden=[]
for relative in filter(None,tracked):
    path=ROOT/relative
    if relative.startswith(('var/','.venv/')) or relative.lower().endswith(('.csv','.pt','.cnf')):forbidden.append(relative)
    if not path.is_file():broken.append(dict(file=relative,target='tracked file missing'));continue
    files.append(dict(path=relative,bytes=path.stat().st_size,sha256=sha(path)))
    if path.suffix=='.md':
        for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)',path.read_text()):
            parsed=urlsplit(target.strip('<>'))
            if parsed.scheme or parsed.netloc or not parsed.path:continue
            resolved=path.parent/unquote(parsed.path)
            if not resolved.exists():broken.append(dict(file=relative,target=target))
integrity=len(frozen)==18 and all(x['status']=='PASS' for x in frozen) and not broken and not forbidden
results.append(dict(gate='G-REPOSITORY-INTEGRITY',status='PASS' if integrity else 'FAIL',scope='18 frozen baseline hashes, tracked artifact hashes, local Markdown targets and forbidden tracked data paths'))
result=dict(status='PASS' if all(x['status']=='PASS' for x in results) else 'INCOMPLETE',
            baseline_version='UBA-DOC-1.0.0',code_commit=commit(),environment_sha256=sha(ROOT/'config/environment.lock.json'),
            observed_at=stamp(),gates=results,frozen_files=frozen,tracked_files=files,broken_links=broken,forbidden_tracked_data=forbidden,
            scope='Evidence and file integrity audit. Detailed acceptance conditions remain defined by docs/11-acceptance.md.')
atomic_json(ROOT/'var/production/delivery-audit.json',result)
print(json.dumps({k:v for k,v in result.items() if k not in ('tracked_files','frozen_files')},ensure_ascii=False))
if result['status']!='PASS':raise SystemExit(1)
