"""本项目隔离库中的真实故障恢复、幂等和锁冲突验证。"""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import time
from orchestration.common import ROOT,load
from orchestration.database import connect
from orchestration.storage import Hdfs

config=load(ROOT/'var/fixtures/test.toml');credentials=ROOT/'var/secrets/test_writer.cnf'
source=ROOT/f'var/fixtures/recovery-{time.time_ns()}.csv'
source.write_text(f'9,{time.time_ns()%10**18},300,pv,1511544070\n')
args=[str(ROOT/'scripts/behaviorctl'),'--config',str(ROOT/'var/fixtures/test.toml'),'--baseline','UBA-DOC-1.0.0','--credentials',str(credentials),'ingest','--source',str(source)]
crashed=subprocess.run(args,env=dict(os.environ,BEHAVIOR_TEST_CRASH_AFTER_RENAME='I00'),capture_output=True,text=True)
assert crashed.returncode==86,(crashed.returncode,crashed.stdout,crashed.stderr)
with connect(config,credentials) as db,db.cursor() as q:
    q.execute('SELECT batch_id FROM data_batch WHERE source_sha256=%s',(hashlib.sha256(source.read_bytes()).hexdigest(),));batch=q.fetchone()['batch_id']
    q.execute('SELECT * FROM task_run WHERE batch_id=%s',(batch,));before=q.fetchall()
    assert len(before)==1 and before[0]['status']=='RUNNING'
    assert Hdfs(config).exists(before[0]['output_path']+'/manifest.json')
resumed=subprocess.run(args,capture_output=True,text=True);assert resumed.returncode==0,(resumed.stdout,resumed.stderr)
a=json.loads(resumed.stdout);assert a['run_id']==before[0]['run_id']
again=subprocess.run(args,capture_output=True,text=True);assert again.returncode==0,(again.stdout,again.stderr)
assert json.loads(again.stdout)['run_id']==a['run_id']
with connect(config,credentials) as db,db.cursor() as q:
    q.execute('SELECT run_id,status,attempt FROM task_run WHERE batch_id=%s',(batch,));after=q.fetchall()
    assert after==[dict(run_id=a['run_id'],status='SUCCEEDED',attempt=1)]
    q.execute('SELECT GET_LOCK(%s,0) AS got',('behavior-heavy-task',));assert q.fetchone()['got']==1
    try:
        blocked=subprocess.run(args,capture_output=True,text=True);assert blocked.returncode==6
        assert json.loads(blocked.stdout)['error_code']=='LOCK_BUSY'
    finally:q.execute('SELECT RELEASE_LOCK(%s)',('behavior-heavy-task',))
print(json.dumps(dict(status='PASS',synthetic=True,batch_id=batch,run_id=a['run_id'],tests=['rename-before-DB recovery','same-run idempotence','global lock conflict']),indent=2))
