"""真实YARN验证空文件和非法UTF8/行为/时间：质量失败不能伪装成空成功。"""
from pathlib import Path
import json
import subprocess
from orchestration.common import ROOT,load
from orchestration.database import connect
from orchestration.pipeline import latest
from orchestration.storage import Hdfs
config=load(ROOT/'var/fixtures/test.toml');credentials=ROOT/'var/secrets/test_writer.cnf'
base=[str(ROOT/'scripts/behaviorctl'),'--config',str(ROOT/'var/fixtures/test.toml'),'--baseline','UBA-DOC-1.0.0','--credentials',str(credentials)]
results=[]
for name,payload,raw,reasons in [('empty',b'',0,{}),('invalid',b'\xff\r\n1,2,3,oops,not_ts\r\n\r\n1,2,3,pv,nope',4,{'BAD_ENCODING':1,'BAD_BEHAVIOR':1,'BAD_FIELD_COUNT':1,'BAD_TIMESTAMP':1})]:
    path=ROOT/f'var/fixtures/{name}.csv';path.write_bytes(payload)
    p=subprocess.run(base+['ingest','--source',str(path)],capture_output=True,text=True);assert p.returncode==0,(p.stdout,p.stderr)
    batch=json.loads(p.stdout)['batch_id']
    p=subprocess.run(base+['profile','--batch-id',batch,'--reducers','1'],capture_output=True,text=True)
    assert p.returncode==4 and json.loads(p.stdout)['error_code']=='QUALITY_GATE_FAILED',(p.returncode,p.stdout,p.stderr)
    with connect(config,credentials) as db,db.cursor() as q:
        q.execute('SELECT status,raw_rows,rejected_rows,accepted_rows FROM data_batch WHERE batch_id=%s',(batch,));r=q.fetchone()
        assert r==dict(status='FAILED',raw_rows=raw,rejected_rows=raw,accepted_rows=0),r
        stage=latest(db,batch,'J01');assert stage and stage['application_id']
    manifest=Hdfs(config).json(stage['output_path']+'/manifest.json')
    assert manifest['counters'].get('REJECTIONS',{})==reasons
    results.append(dict(case=name,batch_id=batch,raw_rows=raw,application_id=stage['application_id'],expected_exit_code=4))
print(json.dumps(dict(status='PASS',synthetic=True,cases=results),indent=2))
