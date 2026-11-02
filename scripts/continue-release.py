#!/usr/bin/env python3
"""Wait for one verified training process, then audit, forecast and publish serially."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,process_start,stamp

p=argparse.ArgumentParser();p.add_argument('--training-identity',type=Path,required=True);args=p.parse_args()
identity=json.loads(args.training_identity.read_text());work=ROOT/'var/production/delivery';work.mkdir(parents=True,exist_ok=True)
def running():
    pid=identity['pid']
    if process_start(pid)!=identity['start_ticks']:return False
    try:return Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()[0]!='Z'
    except OSError:return False
atomic_json(work/'status.json',dict(status='WAITING_FOR_TRAINING',identity=identity,observed_at=stamp()))
while running():time.sleep(2)
models=json.loads((ROOT/'var/production/models.json').read_text())
if models['status']!='SUCCEEDED' or models['experiment_id']!=identity['experiment_id']:
    raise SystemExit('Training ended without the expected successful experiment; no restart or publication attempted')
commands=[('verify-models',['scripts/verify-models.py']),('forecast',['scripts/publish-results.py','forecast']),('publish',['scripts/publish-results.py','publish'])]
for name,command in commands:
    if subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip():raise SystemExit('Working tree changed; commit before continuing formal delivery')
    atomic_json(work/'status.json',dict(status='RUNNING',step=name,observed_at=stamp()))
    with (work/(name+'.stdout.log')).open('w') as out,(work/(name+'.stderr.log')).open('w') as err:
        child=subprocess.Popen([str(ROOT/'.venv/bin/python'),*command],cwd=ROOT,stdout=out,stderr=err)
        result=child.wait()
    if result:
        atomic_json(work/'status.json',dict(status='FAILED',step=name,exit_code=result,observed_at=stamp()))
        raise SystemExit(result)
release=json.loads((ROOT/'var/production/release.json').read_text())
atomic_json(work/'status.json',dict(status='PUBLISHED_APP_AND_M7_VERIFICATION_PENDING',release_id=release['release_id'],observed_at=stamp()))
print(json.dumps({'release_id':release['release_id'],'status':'PUBLISHED_APP_AND_M7_VERIFICATION_PENDING'}))
