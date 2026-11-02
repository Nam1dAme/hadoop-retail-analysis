#!/usr/bin/env python3
"""Start/stop only this project's API/UI using PID + Linux process start identity."""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time
import urllib.request
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,process_start,load

def live(identity):
    return identity and process_start(identity['pid'])==identity['start_ticks']

def main():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['start','stop','status']);args=parser.parse_args()
    config=load(ROOT/'config/development.toml');run=ROOT/'var/run/web';run.mkdir(parents=True,exist_ok=True)
    secret=ROOT/'var/secrets/cursor.key'
    if args.action=='start' and not secret.exists():
        secret.parent.mkdir(parents=True,exist_ok=True)
        with os.fdopen(os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as f:f.write(secrets.token_bytes(32))
    for kind,module in [('api','service'),('ui','app')]:
        path=run/(kind+'.json');identity=json.loads(path.read_text()) if path.exists() else None
        if args.action=='start' and not live(identity):
            logs=ROOT/'var/log/web';logs.mkdir(parents=True,exist_ok=True)
            with (logs/(kind+'.log')).open('ab') as log:
                child=subprocess.Popen([str(ROOT/'.venv/bin/python'),'-m',module],cwd=ROOT,stdout=log,stderr=log,start_new_session=True)
            identity=dict(pid=child.pid,start_ticks=process_start(child.pid));atomic_json(path,identity)
        elif args.action=='stop' and live(identity):
            # Dedicated process group; never stop an unverified PID or another project's service.
            os.killpg(identity['pid'],signal.SIGTERM)
            deadline=time.monotonic()+10
            while live(identity) and time.monotonic()<deadline:time.sleep(.1)
            if live(identity):raise SystemExit('Service termination not confirmed: '+kind)
        print(json.dumps(dict(service=kind,running=bool(live(identity)))))
    if args.action=='start':
        deadline=time.monotonic()+30
        while True:
            try:
                for url in (f'http://127.0.0.1:{config["api"]["port"]}/api/v1/health',f'http://127.0.0.1:{config["ui"]["port"]}/_stcore/health'):
                    with urllib.request.urlopen(url,timeout=2) as r:assert r.status==200
                break
            except Exception:
                if time.monotonic()>=deadline:raise SystemExit('Web readiness failed; inspect var/log/web')
                time.sleep(.3)

if __name__=='__main__':main()
