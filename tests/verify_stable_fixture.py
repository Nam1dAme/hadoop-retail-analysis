"""对稳定版本验收单/双Reducer、排行、分片及审计产物。"""
from pathlib import Path
import json
from orchestration.common import ROOT,load
from orchestration.database import connect
from orchestration.pipeline import latest,validate,files
from orchestration.storage import Hdfs
from ml.shards import ShardDataset
config=load(ROOT/'var/fixtures/test.toml');storage=Hdfs(config)
result=json.loads((ROOT/'var/fixtures/pipeline-stable-result.json').read_text());batch=result['batch_id']
assert result['status']=='SUCCEEDED' and result['analysis']['status']=='PASS'
previous=json.loads((ROOT/'var/fixtures/pipeline-a-result.json').read_text())
for k in ('rows',):assert result['quality'][k]==previous['quality'][k]
for k,v in previous['quality']['profile'].items():assert result['quality']['profile'][k]==v
with connect(config,ROOT/'var/secrets/test_writer.cnf') as db:
    stages={}
    for s in result['stages']:
        row=latest(db,batch,s);stages[s]=validate(storage,row['output_path'])
        assert stages[s]['config']['parameters']['reducers']==1
        assert stages[s]['application_id']!=previous['stages'].get(s,{}).get('application_id')
    for s,dataset in [('J03','event_cube'),('J03','entity_daily'),('J04users','user_daily'),('J04summary','range_summary'),('J04summary','activity_daily'),('J05',None)]:
        before=previous['stages'][s]['run_id']
        with db.cursor() as q:q.execute('SELECT output_path FROM task_run WHERE run_id=%s',(before,));path=q.fetchone()['output_path']
        old=validate(storage,path)
        assert sorted(storage.dump(files(old,dataset)))==sorted(storage.dump(files(stages[s],dataset))),(s,dataset)
    rankings={}
    for line in storage.dump(files(stages['J03rank'])):
        period,kind,behavior,rank,entity,count=line.split('\t')
        rankings.setdefault((period,kind,behavior),[]).append((int(rank),entity,int(count)))
    assert sorted(rankings[('ALL','ITEM','pv')])==[(1,'20',2),(2,'10',1),(3,'30',1)]
prepared=json.loads((ROOT/'var/fixtures/prepare-stable-result.json').read_text());assert prepared['status']=='SUCCEEDED'
cache=Path(prepared['samples']['cache_path']);seen=[]
for split in ('TRAIN','VALIDATION','TEST','FORECAST'):
    data=ShardDataset(cache/split);keys=[k.decode() for b in data.batches(batch_size=2,epoch=0) for k in b['sample_key']]
    assert len(keys)==prepared['samples']['splits'][split]['rows']
    assert len(set(keys))==len(keys);seen+=keys
assert len(seen)==len(set(seen))==5
print(json.dumps({'status':'PASS','synthetic':True,'batch_id':batch,'tests':['CSV small splits and no final LF','1-vs-2 reducer semantic equivalence','exact UV and retention','full-window TopK and tie order','independent population audit','mmap shard row/key verification']},indent=2))
