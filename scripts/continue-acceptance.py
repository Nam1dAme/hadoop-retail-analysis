#!/usr/bin/env python3
"""Serial actual acceptance; every failure stops the queue, without training retries."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,process_start,stamp,sha,commit

STEPS=('verify-release','browser','api-load','hotspot','scale','backup','restore')
p=argparse.ArgumentParser();p.add_argument('--publisher-identity',type=Path);p.add_argument('--steps',nargs='+',choices=STEPS,default=STEPS);args=p.parse_args()
work=ROOT/'var/production/acceptance';work.mkdir(parents=True,exist_ok=True)
status=dict(status='RUNNING',requested_steps=args.steps,results=[],started_at=stamp(),independent_backup_verified=False)
def save():atomic_json(work/'status.json',status)
if args.publisher_identity:
    identity=json.loads(args.publisher_identity.read_text());status['status']='WAITING_FOR_PUBLICATION';save()
    while process_start(identity['pid'])==identity['start_ticks']:
        try:
            if Path(f'/proc/{identity["pid"]}/stat').read_text().split(') ',1)[1].split()[0]=='Z':break
        except OSError:break
        time.sleep(2)
release=ROOT/'var/production/release.json'
if not release.exists():
    status.update(status='FAILED',step='publication',reason='No successful publication index',finished_at=stamp());save();raise SystemExit(1)
status['release_id']=json.loads(release.read_text())['release_id']
python=str(ROOT/'.venv/bin/python')
commands={
 'verify-release':([python,'scripts/verify-release.py'],ROOT/'var/production/release-verification.json'),
 'browser':(['/home/hadoop/hadoop-lab/.venv/bin/python','scripts/verify-browser.py'],ROOT/'var/production/browser/verification.json'),
 'api-load':([python,'scripts/load-test-api.py'],ROOT/'var/production/api-load-test.json'),
 'hotspot':([python,'tests/verify_hotspot_yarn.py'],ROOT/'var/fixtures/hotspot-verification.json'),
 'scale':([python,'scripts/run-scale.py'],ROOT/'var/scale/verification.json'),
 'backup':([python,'scripts/backup-release.py'],ROOT/'var/production/backup.json'),
 'restore':([python,'scripts/restore-backup.py'],ROOT/'var/production/recovery.json')}
for step in args.steps:
    if subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip():
        status.update(status='FAILED',step=step,reason='Working tree changed',finished_at=stamp());save();raise SystemExit(1)
    status.update(status='RUNNING',step=step,observed_at=stamp());save()
    command,report=commands[step];env=dict(os.environ)
    if step=='browser':env['PLAYWRIGHT_BROWSERS_PATH']='/home/hadoop/hadoop-lab/var/cache/playwright'
    result=dict(step=step,command=command,code_commit=commit(),started_at=stamp())
    with (work/(step+'.stdout.log')).open('w') as out,(work/(step+'.stderr.log')).open('w') as err:
        child=subprocess.Popen(command,cwd=ROOT,env=env,stdout=out,stderr=err)
        result.update(pid=child.pid,start_ticks=process_start(child.pid));status['active_process']=result;save()
        result['exit_code']=child.wait()
    result['finished_at']=stamp()
    valid=not result['exit_code'] and report.exists() and json.loads(report.read_text()).get('status')=='PASS'
    result['status']='PASS' if valid else 'FAIL'
    if valid:result.update(report=str(report),report_sha256=sha(report))
    status['results'].append(result);status.pop('active_process',None);save()
    if not valid:
        status.update(status='FAILED',finished_at=stamp());save();raise SystemExit(1)
status.update(status='REQUESTED_STEPS_PASS',finished_at=stamp(),note='Independent physical backup remains unverified; this status is not all-gates acceptance.');save()
print(json.dumps(status))
