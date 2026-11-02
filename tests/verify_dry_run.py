from pathlib import Path
import json,subprocess
from orchestration.common import ROOT,load
from orchestration.database import connect
config=load(ROOT/'var/fixtures/test.toml');cred=ROOT/'var/secrets/test_writer.cnf'
with connect(config,cred) as db,db.cursor() as q:
 def snapshot():
  q.execute('SELECT COUNT(*) AS n FROM task_run');tasks=q.fetchone()['n']
  q.execute('SELECT COUNT(*) AS n FROM data_batch');batches=q.fetchone()['n']
  q.execute('SELECT generation FROM resource_lease');generation=q.fetchone()['generation']
  return tasks,batches,generation
 before=snapshot()
 base=[str(ROOT/'scripts/behaviorctl'),'--config',str(ROOT/'var/fixtures/test.toml'),'--baseline','UBA-DOC-1.0.0','--credentials',str(cred),'--dry-run']
 p=subprocess.run(base+['pipeline','--batch-id','97eb0c70-44cb-4a7a-bf9d-608bd9475b2f'],capture_output=True,text=True)
 assert p.returncode==0,(p.stdout,p.stderr)
 result=json.loads(p.stdout);assert result['status']=='DRY_RUN' and result['dependencies_checked']
 p=subprocess.run(base+['pipeline','--batch-id','00000000-0000-0000-0000-000000000000'],capture_output=True,text=True)
 assert p.returncode==2 and json.loads(p.stdout)['error_code']=='INVALID_CONFIG'
 assert snapshot()==before
print(json.dumps({'status':'PASS','tests':['read-only prerequisite validation','missing batch rejected','no task/batch/lease writes']}))
