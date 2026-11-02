from datetime import datetime
from pathlib import Path
import hashlib
import json
import numpy as np
import pytest
from ingest.source import examine
from ml.shards import ShardWriter,ShardDataset
from orchestration.common import ConfigError,load

def sample(i,label=0):
    return dict(sample_key=hashlib.sha256(('00'+str(i)+'|1512057600').encode()).hexdigest(),user_id='00'+str(i),as_of=1512057600,length=1,category=[7]+[0]*127,behavior=[1]+[0]*127,hour=[1]+[0]*127,gap=[0.0]*128,numeric=list(range(14)),label=label,history_event_count=1,cohort='SEEN_TRAIN')

def test_source_bytes_and_lines(tmp_path):
    p=tmp_path/'input.csv';p.write_bytes(b'a\r\nb\n\ninvalid\xff')
    info=examine(p)
    assert info['rows']==4 and info['sha256']==hashlib.sha256(p.read_bytes()).hexdigest()
    assert info['downloaded_at'] is None
    p.write_bytes(b'');assert examine(p)['rows']==0

def test_shards_tail_shuffle_mmap_and_identity(tmp_path):
    w=ShardWriter(tmp_path/'TRAIN',[0]*14,[1]*14,capacity=3)
    for i in range(8):w.add(sample(i,i%2))
    m=w.finish();assert [s['rows'] for s in m['shards']]==[3,3,2]
    assert m['rows']==8 and m['positive_count']==4
    data=ShardDataset(tmp_path/'TRAIN')
    a=list(data.batches(2,epoch=1));b=list(data.batches(2,epoch=1));c=list(data.batches(2,epoch=2))
    ids=lambda batches:[x.decode() for batch in batches for x in batch['sample_key']]
    assert ids(a)==ids(b) and set(ids(a))=={sample(i)['sample_key'] for i in range(8)}
    assert len(ids(a))==len(set(ids(a)))==8
    assert ids(a)!=ids(c)
    for batch in a:
        assert batch['numeric'].dtype==np.float32 and np.all(batch['numeric'][:,3]==3)
    path=tmp_path/'TRAIN/shard-00002'
    assert np.load(path/'category.npy',mmap_mode='r').shape==(2,128)
    offsets=np.load(path/'user_offsets.npy');raw=(path/'user_ids.utf8').read_bytes()
    assert raw[offsets[0]:offsets[1]].decode()=='006'


def test_forecast_and_zero_std(tmp_path):
    w=ShardWriter(tmp_path/'FORECAST',[1]*14,[2]*14,capacity=1);w.add(sample(0,-1));m=w.finish()
    assert m['positive_count']==m['negative_count']==0
    batch=next(ShardDataset(tmp_path/'FORECAST').batches())
    assert batch['label'][0]==-1 and batch['numeric'][0,0]==-.5

@pytest.mark.parametrize('db,root,work',[
    ('behavior_analytics','/behavior-test/m123','var/tests/m123'),
    ('behavior_test_m123','/behavior/v1','var/tests/m123'),
    ('behavior_test_m123','/behavior-test/m123','var/production'),
])
def test_synthetic_requires_three_way_isolation(tmp_path,db,root,work):
    from orchestration.common import ROOT
    cfg=tmp_path/'config.toml'
    cfg.write_text(f'''baseline_version="UBA-DOC-1.0.0"
synthetic=true
[database]
name="{db}"
[hdfs]
root="{root}"
[paths]
work_dir="{ROOT/work}"
''')
    with pytest.raises(ConfigError):load(cfg)

def test_code_identity_ignores_document_only_changes(tmp_path,monkeypatch):
    import orchestration.common as common
    monkeypatch.setattr(common,'ROOT',tmp_path)
    (tmp_path/'orchestration').mkdir();code=tmp_path/'orchestration/worker.py';code.write_text('x=1\n')
    identity=common.implementation_fingerprint()
    (tmp_path/'docs').mkdir();(tmp_path/'docs/report.md').write_text('new execution evidence')
    assert common.implementation_fingerprint()==identity
    code.write_text('x=2\n');assert common.implementation_fingerprint()!=identity

def test_last_effective_batch_gradient_uses_actual_population(tmp_path):
    import torch
    from ml.shards import training_groups
    torch.set_num_threads(1)
    w=ShardWriter(tmp_path/'TRAIN',[0]*14,[1]*14,capacity=5)
    for i in range(13):w.add(sample(i,i%2))
    w.finish();dataset=ShardDataset(tmp_path/'TRAIN')
    groups=list(training_groups(dataset,micro_batch=4,effective_batch=8))
    assert [n for _,_,n in groups]==[8,5]
    observed=[]
    for batches,weights,n in groups:
        assert sum(len(b['label']) for b in batches)==n and sum(weights)==pytest.approx(1)
        observed.extend(k for b in batches for k in b['sample_key'])
        model=torch.nn.Linear(14,1,dtype=torch.float64)
        model.zero_grad()
        for batch,weight in zip(batches,weights):
            x=torch.from_numpy(batch['numeric']).double();y=torch.from_numpy(batch['label']).double()
            (torch.nn.functional.binary_cross_entropy_with_logits(model(x).squeeze(-1),y)*weight).backward()
        actual=model.weight.grad.clone();model.zero_grad()
        x=torch.from_numpy(np.concatenate([b['numeric'] for b in batches])).double()
        y=torch.from_numpy(np.concatenate([b['label'] for b in batches])).double()
        torch.nn.functional.binary_cross_entropy_with_logits(model(x).squeeze(-1),y).backward()
        assert torch.allclose(model.weight.grad,actual,atol=1e-12,rtol=0)
    assert len(observed)==len(set(observed))==13

def test_orphan_java_is_identified_by_staging_path(tmp_path,monkeypatch):
    import orchestration.control as control
    from pathlib import Path as RealPath
    run='12345678-1234-1234-1234-123456789abc'
    proc=tmp_path/'proc';entry=proc/'42';entry.mkdir(parents=True)
    command=entry/'cmdline'
    command.write_bytes(b'/jdk/bin/java\0FsShell\0-put\0/input.csv\0/root/.source-'+run.encode()+b'.tmp/source.csv\0')
    monkeypatch.setattr(control,'Path',lambda p:proc if p=='/proc' else RealPath(p))
    assert control.live_java(run)
    assert not control.live_java('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
    command.write_bytes(b'/usr/bin/python\0'+run.encode()+b'\0')
    assert not control.live_java(run)

def test_bundle_validation_checks_nested_manifests():
    from orchestration.pipeline import validate
    from orchestration.common import BASELINE,fingerprint,ManifestError
    cfg={'stage':'E01'}
    nested={'path':'/bundle/shards/TRAIN/manifest.json','sha256':'a'*64,'bytes':12,'rows':0}
    m={'baseline_version':BASELINE,'status':'SUCCEEDED','stage':'E01','config':cfg,'config_sha256':fingerprint(cfg),'inputs':[],'input_hash':fingerprint([]),'outputs':[nested]}
    class Storage:
        def json(self,p):return m
        def inspect(self,p):return [dict(nested),{'path':'/bundle/manifest.json','sha256':'b'*64,'bytes':42,'rows':0}]
    assert validate(Storage(),'/bundle')==m
    class Damaged(Storage):
        def inspect(self,p):return [dict(nested,sha256='c'*64)]
    with pytest.raises(ManifestError):validate(Damaged(),'/bundle')
