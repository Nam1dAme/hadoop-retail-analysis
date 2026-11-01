"""通过后才可将合成YARN链路记为PASS；不作为全量证据。"""
from collections import Counter
from pathlib import Path
import json
import sys
from orchestration.common import load,ROOT,sha
from orchestration.database import connect
from orchestration.storage import Hdfs
from orchestration.pipeline import latest,validate,files

config=load(ROOT/'var/fixtures/test.toml');hdfs=Hdfs(config)
batch=sys.argv[1]
with connect(config,ROOT/'var/secrets/test_writer.cnf') as db:
    outputs={}
    for stage in ('J01','J02','J03','J04users','J04summary','J05','J06'):
        row=latest(db,batch,stage);assert row,stage
        outputs[stage]=validate(hdfs,row['output_path'])
        assert outputs[stage]['application_id'].startswith('application_')
quality=json.loads((Path(config['paths']['work_dir'])/'batches'/batch/'quality.json').read_text())
assert quality['rows']==dict(raw_rows=9,accepted_rows=8,rejected_rows=1,in_scope_rows=7,outside_rows=1)
assert not quality['gate_passed'] and quality['profile']['duplicate_extra']==1
positions=[]
for line in hdfs.dump(files(outputs['J01'],'facts')):
    a=line.split('\t');assert a[0]==a[2];positions.append(int(a[0]))
raw=(ROOT/'var/fixtures/sample-a.csv').read_bytes();offset=0;expected=[]
for line in raw.splitlines(keepends=True):
    if b'oops' not in line:expected.append(offset)
    offset+=len(line)
assert sorted(positions)==expected and len(set(positions))==8
cube={}
for line in hdfs.dump(files(outputs['J03'],'event_cube')):
    day,hour,behavior,n=line.split('\t');cube[(day,int(hour),behavior)]=int(n)
assert sum(cube.values())==7
counts=Counter()
for (_,_,b),n in cube.items():counts[b]+=n
assert counts==dict(pv=4,buy=1,cart=1,fav=1)
entity=Counter()
for line in hdfs.dump(files(outputs['J03'],'entity_daily')):
    day,kind,identity,b,n=line.split('\t');entity[kind]+=int(n)
assert entity==dict(ITEM=7,CATEGORY=7)
ranges={}
for line in hdfs.dump(files(outputs['J04summary'],'range_summary')):
    a,b,*values=line.split('\t');ranges[(a,b)]=list(map(int,values))
assert len(ranges)==45
assert ranges[('2017-11-25','2017-11-27')]==[3,3,1,7]
assert ranges[('2017-11-25','2017-11-26')]==[2,2,1,5]
activity={}
for line in hdfs.dump(files(outputs['J04summary'],'activity_daily')):
    d,*values=line.split('\t');activity[d]=list(map(int,values))
assert len(activity)==9 and activity['2017-11-25']==[2,2,1]
assert activity['2017-12-03']==[0,0,-1]
rows=[json.loads(line.split('\t',1)[1]) for line in hdfs.dump(files(outputs['J05']))]
assert len({r['sample_key'] for r in rows})==len(rows)
train=[r for r in rows if r['split']=='TRAIN']
assert all(r['label']==0 for r in train)
assert all(len(r['category'])==128 and len(r['numeric'])==14 for r in rows)
import numpy as np
normalization=json.loads(next(hdfs.dump(files(outputs['J06']))).split('\t',1)[1])
a=np.array([r['numeric'] for r in train]);std=a.std(axis=0);std[std==0]=1
assert np.allclose(a.mean(axis=0),normalization['mean'],atol=1e-12)
assert np.allclose(std,normalization['std'],atol=1e-12)
print(json.dumps({'status':'PASS','synthetic':True,'batch_id':batch,'stages':list(outputs),'source_sha256':sha(ROOT/'var/fixtures/sample-a.csv'),'tests':['T01','T02','T04','T05','T07']},indent=2))
