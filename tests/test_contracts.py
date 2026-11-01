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
    return dict(sample_key=hashlib.sha256(str(i).encode()).hexdigest(),user_id='00'+str(i),as_of=1512057600,length=1,category=[7]+[0]*127,behavior=[1]+[0]*127,hour=[1]+[0]*127,gap=[0.0]*128,numeric=list(range(14)),label=label,history_event_count=1,cohort='SEEN_TRAIN')

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
