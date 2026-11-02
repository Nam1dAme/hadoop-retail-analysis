#!/usr/bin/env python3
"""Run 1M/10M/full actual YARN analysis; sampled records never enter model training."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import statistics
import urllib.request
import urllib.error
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,stamp
from orchestration.sampling import record_samples
from orchestration.control import Lease

parser=argparse.ArgumentParser();parser.add_argument('--tiers',nargs='+',choices=['1000000','10000000','full'],default=['1000000','10000000','full']);args=parser.parse_args()
if subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip():raise SystemExit('Commit changes before scale execution')
work=ROOT/'var/scale';work.mkdir(parents=True,exist_ok=True)
original=json.loads((ROOT/'var/production/m123-verification.json').read_text())['source']
source=load(ROOT/'config/development.toml')['source']['path']
with Lease(load(ROOT/'config/development.toml'),ROOT/'var/secrets/writer.cnf') as sampling_lease:
    sampling=record_samples(source,work/'inputs',original['sha256'],original['rows'],guard=sampling_lease.guard)

def history(tasks):
    results=[]
    def get(url):
        with urllib.request.urlopen('http://127.0.0.1:19888/ws/v1/history/mapreduce/jobs/'+url,timeout=10) as r:return json.load(r)
    for task in tasks:
        if not task['application_id']:continue
        job=task['application_id'].replace('application_','job_',1);deadline=time.monotonic()+120
        while True:
            try:
                rows=get(job+'/tasks')['tasks']['task'];break
            except (urllib.error.HTTPError,KeyError):
                if time.monotonic()>deadline:raise
                time.sleep(2)
        if isinstance(rows,dict):rows=[rows]
        if not rows or any(r['state']!='SUCCEEDED' for r in rows):raise RuntimeError('Task history is incomplete')
        result=dict(stage=task['stage'],application_id=task['application_id'],task_count=len(rows),task_durations={})
        for kind in ('MAP','REDUCE'):
            durations=[r['elapsedTime'] for r in rows if r['type']==kind]
            result['task_durations'][kind]=dict(count=len(durations),max_millis=max(durations,default=0),median_millis=statistics.median(durations) if durations else None)
        reducers=[]
        for row in rows:
            if row['type']!='REDUCE':continue
            counters=get(job+'/tasks/'+row['id']+'/counters')['jobTaskCounters']
            input_records=None
            for group in counters.get('taskCounterGroup',[]):
                for counter in group.get('counter',[]):
                    if counter['name']=='REDUCE_INPUT_RECORDS':input_records=counter.get('value',counter.get('totalCounterValue'))
            reducers.append(dict(task_id=row['id'],input_records=input_records,elapsed_millis=row['elapsedTime']))
        result['reducers']=reducers;results.append(result)
    return results
results=[]
for tier in args.tiers:
    root=work/tier;root.mkdir(parents=True,exist_ok=True)
    path=source if tier=='full' else sampling['samples'][tier]['path']
    config=root/'config.toml'
    config.write_text(f'''baseline_version = "UBA-DOC-1.0.0"
benchmark = true
sampled = {'false' if tier=='full' else 'true'}
[source]
path = "{path}"
[database]
host = "127.0.0.1"
port = 3307
name = "behavior_scale"
[hdfs]
uri = "hdfs://127.0.0.1:19000"
root = "/behavior-scale/v1/{tier}"
[paths]
work_dir = "{root}"
''')
    base=[str(ROOT/'scripts/behaviorctl'),'--config',str(config),'--baseline','UBA-DOC-1.0.0']
    def run(name,options,credentials='writer'):
        with (root/(name+'.json')).open('w') as out,(root/(name+'.log')).open('w') as err:
            result=subprocess.run(base+['--credentials',str(ROOT/f'var/secrets/scale_{credentials}.cnf')]+options,stdout=out,stderr=err)
        if result.returncode:raise SystemExit(f'{tier} {name} failed; inspect {root}')
        value=json.loads((root/(name+'.json')).read_text());assert value['status']=='SUCCEEDED'
        return value
    started=time.monotonic();begin=stamp()
    run('migrate',['migrate'],'migrator')
    batch=run('ingest',['ingest','--source-url','https://tianchi.aliyun.com/dataset/649'])['batch_id']
    outcome=run('pipeline',['pipeline','--batch-id',batch,'--through','J04summary','--reducers','2'])
    status=run('status',['status','--batch-id',batch])
    actual=outcome['quality']['rows']['raw_rows'];expected=original['rows'] if tier=='full' else int(tier)
    assert actual==expected
    report=dict(tier=tier,status='PASS',sampled=tier!='full',synthetic=False,batch_id=batch,rows=actual,
                started_at=begin,finished_at=stamp(),seconds=time.monotonic()-started,
                scope='M1-M2 analysis throughput; record samples are not prediction evidence',
                quality=outcome['quality'],tasks=status['tasks'],task_history=history(status['tasks']))
    atomic_json(root/'verification.json',report);results.append(report)
    print(json.dumps({'tier':tier,'status':'PASS','seconds':report['seconds']}),flush=True)
atomic_json(work/'verification.json',dict(status='PASS',tiers=results,sampling=sampling,observed_at=stamp()))
