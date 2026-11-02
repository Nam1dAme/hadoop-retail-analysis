"""在三重隔离配置中验证真实YARN失败、attempt链及成功阶段复用。"""
from pathlib import Path
import json,os,subprocess,time
from orchestration.common import ROOT,load
from orchestration.database import connect
from orchestration.storage import application
config=load(ROOT/'var/fixtures/test.toml');cred=ROOT/'var/secrets/test_writer.cnf'
p=ROOT/f'var/fixtures/yarn-retry-{time.time_ns()}.csv';item=str(time.time_ns()%10**18)
p.write_text(f'1,{item},100,pv,1511544070\n1,{item},100,buy,1511544071\n')
base=[str(ROOT/'scripts/behaviorctl'),'--config',str(ROOT/'var/fixtures/test.toml'),'--baseline','UBA-DOC-1.0.0','--credentials',str(cred)]
r=subprocess.run(base+['ingest','--source',str(p)],capture_output=True,text=True);assert r.returncode==0,(r.stdout,r.stderr)
batch=json.loads(r.stdout)['batch_id'];args=base+['pipeline','--batch-id',batch,'--through','J03','--reducers','1']
r=subprocess.run(args,env=dict(os.environ,BEHAVIOR_TEST_FAIL_STAGE='J03'),capture_output=True,text=True);assert r.returncode==5,(r.returncode,r.stdout,r.stderr)
with connect(config,cred) as db,db.cursor() as q:
 q.execute('SELECT * FROM task_run WHERE batch_id=%s AND stage=%s',(batch,'J03'));failed=q.fetchone()
 assert failed['status']=='FAILED' and failed['attempt']==1 and failed['application_id']
 app=application(config,failed['application_id']);assert app['state'] in ('FINISHED','FAILED','KILLED') and app['finalStatus']=='FAILED'
 q.execute("SELECT run_id FROM task_run WHERE batch_id=%s AND stage='J01' AND status='SUCCEEDED'",(batch,));before=q.fetchone()['run_id']
r=subprocess.run(args,capture_output=True,text=True);assert r.returncode==0,(r.stdout,r.stderr)
with connect(config,cred) as db,db.cursor() as q:
 q.execute("SELECT * FROM task_run WHERE batch_id=%s AND stage='J03' ORDER BY attempt",(batch,));rows=q.fetchall()
 assert len(rows)==2 and [x['status'] for x in rows]==['FAILED','SUCCEEDED']
 assert rows[1]['attempt']==2 and rows[1]['parent_run_id']==rows[0]['run_id']
 q.execute("SELECT run_id FROM task_run WHERE batch_id=%s AND stage='J01'",(batch,));assert q.fetchall()==[{'run_id':before}]
print(json.dumps({'status':'PASS','synthetic':True,'batch_id':batch,'failed_application':failed['application_id'],'retry_application':rows[1]['application_id'],'tests':['real YARN failure','terminal verification','attempt-2 parent chain','successful predecessor reused']},indent=2))
