"""Actual isolated MariaDB + filesystem interruption/recovery integration test."""
import json
from pathlib import Path
import sys
import uuid
import numpy as np
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,now,fingerprint,atomic_json,PipelineError
from orchestration.database import connect,migrate
from orchestration.control import Lease
from ml.shards import ShardWriter,ShardDataset
from ml.training import fit,trusted_load

config=load(ROOT/'var/fixtures/test.toml')
with connect(config,ROOT/'var/secrets/test_migrator.cnf') as db:migrate(db)
directory=ROOT/'var/tests/model-recovery'/str(uuid.uuid4());directory.mkdir(parents=True)
for split in ('TRAIN','VALIDATION'):
 writer=ShardWriter(directory/split,[0]*14,[1]*14)
 for i in range(64):
  user=str(i+1);anchor=1511971200
  import hashlib
  row=dict(sample_key=hashlib.sha256(f'{user}|{anchor}'.encode()).hexdigest(),user_id=user,as_of=anchor,
           length=3,category=[1]*3+[0]*125,behavior=[1]*3+[0]*125,hour=[1]*3+[0]*125,
           gap=[0]*128,numeric=[(-1 if i%2==0 else 1)]+[0]*13,label=i%2,history_event_count=3,cohort='SEEN_TRAIN')
  writer.add(row)
 writer.finish()
train=ShardDataset(directory/'TRAIN');validation=ShardDataset(directory/'VALIDATION')
identities=[]
with Lease(config,ROOT/'var/secrets/test_writer.cnf') as lease:
 with lease.db.cursor() as q:
  q.execute('SELECT batch_id FROM data_batch LIMIT 1');batch=q.fetchone()['batch_id']
  for i in range(2):
   experiment=str(uuid.uuid4());identities.append(experiment)
   q.execute('INSERT INTO experiment VALUES(%s,%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,%s)',
             (experiment,batch,'synthetic-model-recovery',str(directory),fingerprint([str(directory),i]),fingerprint({}),'PENDING','{}',now(),now()))
 import os
 os.environ['BEHAVIOR_TEST_STOP_AFTER_EPOCH']='1'
 try:fit(lease,identities[0],train,validation,'LOGISTIC',.001,directory,'synthetic')
 except PipelineError as error:assert 'recovery test' in str(error)
 else:raise AssertionError('Expected actual interruption')
 finally:os.environ.pop('BEHAVIOR_TEST_STOP_AFTER_EPOCH')
 recovered=fit(lease,identities[0],train,validation,'LOGISTIC',.001,directory,'synthetic')
 uninterrupted=fit(lease,identities[1],train,validation,'LOGISTIC',.001,directory,'synthetic')
 left=trusted_load(recovered['checkpoint_path'],recovered['checkpoint_hash'],directory)
 right=trusted_load(uninterrupted['checkpoint_path'],uninterrupted['checkpoint_hash'],directory)
 assert all(torch.equal(left[k],right[k]) for k in left)
 assert recovered['validation_metrics']==uninterrupted['validation_metrics']
 assert recovered['model_run_id']!=uninterrupted['model_run_id']
 for result in (recovered,uninterrupted):
  with lease.db.cursor() as q:
   q.execute('SELECT epoch,sample_count FROM model_epoch WHERE model_run_id=%s ORDER BY epoch',(result['model_run_id'],));epochs=q.fetchall()
   assert [x['epoch'] for x in epochs]==list(range(1,len(epochs)+1))
   assert all(x['sample_count']==64 for x in epochs)
 report=dict(status='PASS',synthetic=True,experiments=identities,tests=['actual epoch-boundary interruption','same-model resume','bitwise-equal recovered vs uninterrupted best weights','no duplicated epoch records','all samples each epoch'])
 atomic_json(ROOT/'var/fixtures/model-recovery-verification.json',report)
 print(json.dumps(report),flush=True)
