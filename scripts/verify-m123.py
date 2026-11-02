#!/usr/bin/env python3
"""完整源的产物、真实YARN状态、独立参考和全量mmap遍历验收。"""
import hashlib
import json
from pathlib import Path
import sys
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,sha,stamp,commit,atomic_json
from orchestration.database import connect
from orchestration.pipeline import validate
from orchestration.storage import Hdfs,application
from ml.shards import ShardDataset

started=time.monotonic();config=load(ROOT/'config/development.toml');work=ROOT/'var/production'
source=json.loads((work/'ingest.json').read_text());pipeline=json.loads((work/'pipeline.json').read_text());samples=json.loads((work/'samples.json').read_text());batch=source['batch_id']
assert source['status']==pipeline['status']==samples['status']=='SUCCEEDED'
assert pipeline['batch_id']==samples['batch_id']==batch
storage=Hdfs(config)
with connect(config,ROOT/'var/secrets/writer.cnf') as db,db.cursor() as q:
    q.execute('SELECT * FROM data_batch WHERE batch_id=%s',(batch,));batch_row=q.fetchone();assert batch_row['status']=='SUCCEEDED'
    q.execute('SELECT * FROM task_run WHERE batch_id=%s ORDER BY created_at',(batch,));tasks=q.fetchall()
    assert tasks and all(x['status']=='SUCCEEDED' for x in tasks)
    q.execute('SELECT * FROM experiment WHERE experiment_id=%s',(samples['experiment_id'],));experiment=q.fetchone();assert experiment['status']=='PENDING' and experiment['selected_model_id'] is None
artifacts=[]
for row in tasks:
    m=validate(storage,row['output_path'],{'run_id':row['run_id'],'batch_id':batch,'code_commit':row['code_commit'],'input_hash':row['input_hash'],'config_sha256':row['config_hash'],'environment_sha256':row['environment_hash']})
    if row['application_id']:
        app=application(config,row['application_id']);assert app['state']=='FINISHED' and app['finalStatus']=='SUCCEEDED'
        assert app['name']=='behavior-'+row['stage']+'-'+row['run_id']
    counters=m.get('counters',{});task_counters=counters.get('org.apache.hadoop.mapreduce.TaskCounter',{})
    item={'stage':row['stage'],'run_id':row['run_id'],'application_id':row['application_id'],'producer_commit':row['code_commit'],'seconds':(row['finished_at']-row['started_at']).total_seconds(),'files':len(m['outputs']),'bytes':sum(f['bytes'] for f in m['outputs']),'rows_by_file':sum(f['rows'] for f in m['outputs'] if 'key_class' in f),'cpu_millis':task_counters.get('CPU_MILLISECONDS'),'gc_millis':task_counters.get('GC_TIME_MILLIS'),'map_rss_max_bytes':task_counters.get('MAP_PHYSICAL_MEMORY_BYTES_MAX'),'reduce_rss_max_bytes':task_counters.get('REDUCE_PHYSICAL_MEMORY_BYTES_MAX')}
    artifacts.append(item);print(json.dumps({'verified':row['stage'],'files':item['files']},ensure_ascii=False),flush=True)
assert len([x for x in artifacts if x['application_id']])==8
reference_path=Path('/home/hadoop/hadoop-lab/var/dataset-assessment-20261101/profile.json')
reference=json.loads(reference_path.read_text());assert reference['source_sha256']==source['source']['sha256']
rows=pipeline['quality']['rows'];profile=pipeline['quality']['profile'];assert pipeline['quality']['gate_passed']
assert rows['raw_rows']==rows['accepted_rows']==reference['rows']==100150807 and rows['rejected_rows']==0
for k,r in [('user_count_all','users'),('item_count_all','items'),('category_count_all','categories'),('timestamp_min','timestamp_min'),('timestamp_max','timestamp_max')]:assert profile[k]==reference[r]
assert pipeline['analysis']['status']=='PASS' and pipeline['analysis']['event_counts']==reference['in_window_behaviors']
assert rows['outside_rows']==sum(reference['out_of_window_by_behavior'].values())
actual={};cache=Path(samples['samples']['cache_path'])
for split,expected in samples['samples']['splits'].items():
    data=ShardDataset(cache/split,verify=True);count=positive=negative=seen=0;digest=hashlib.sha256()
    for b in data.batches(batch_size=512,shuffle=False):
        count+=len(b['label']);positive+=int((b['label']==1).sum());negative+=int((b['label']==0).sum());seen+=int(b['cohort'].sum())
        for k in b['sample_key']:digest.update(bytes(k)+b'\n')
    assert count==expected['rows'] and positive==expected['positive_count'] and negative==expected['negative_count']
    assert seen==expected['seen_count'] and count-seen==expected['unseen_count']
    assert digest.hexdigest()==expected['sample_order_sha256']
    actual[split]={'rows':count,'positive_count':positive,'negative_count':negative,'seen_count':seen,'unseen_count':count-seen,'shards':len(data.manifest['shards']),'ordered_epoch_sha256':digest.hexdigest()}
    print(json.dumps({'verified_split':split,'rows':count}),flush=True)
report={'status':'PASS','baseline_version':'UBA-DOC-1.0.0','verified_at':stamp(),'verifier_commit':commit(),'verifier_sha256':sha(Path(__file__)),'batch_id':batch,'experiment_id':samples['experiment_id'],'source':source['source'],'quality':pipeline['quality'],'analysis':pipeline['analysis'],'samples':actual,'artifacts':artifacts,'reference':{'path':str(reference_path),'sha256':sha(reference_path),'source_sha256':reference['source_sha256']},'pipeline_seconds':sum(x['seconds'] for x in artifacts),'verification_seconds':time.monotonic()-started,'business_training_performed':False,'external_data_distribution_performed':False}
atomic_json(work/'m123-verification.json',report)
print(json.dumps({'status':'PASS','report':str(work/'m123-verification.json')},ensure_ascii=False))
