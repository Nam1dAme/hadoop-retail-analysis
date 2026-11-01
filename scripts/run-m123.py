#!/usr/bin/env python3
"""完整真实源M1—M3执行入口；各阶段输出保留在var/production，不混入合成测试。"""
from pathlib import Path
import json
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
work=ROOT/'var/production';work.mkdir(parents=True,exist_ok=True)
base=[str(ROOT/'scripts/behaviorctl'),'--baseline','UBA-DOC-1.0.0']
def run(name,args):
    path=work/(name+'.json')
    with path.open('w') as out,(work/(name+'.stderr.log')).open('w') as err:
        p=subprocess.run(base+args,stdout=out,stderr=err)
    if p.returncode:raise SystemExit(f'{name} failed ({p.returncode}); inspect {path}')
    value=json.loads(path.read_text());assert value['status']=='SUCCEEDED'
    print(json.dumps({'step':name,'status':'SUCCEEDED','batch_id':value.get('batch_id')},ensure_ascii=False),flush=True)
    return value
source=run('ingest',['ingest','--source-url','https://tianchi.aliyun.com/dataset/649'])
batch=source['batch_id']
run('pipeline',['pipeline','--batch-id',batch,'--through','J06','--reducers','2'])
run('samples',['prepare-samples','--batch-id',batch])
run('status',['status','--batch-id',batch])
