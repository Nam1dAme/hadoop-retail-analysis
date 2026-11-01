from pathlib import Path
import hashlib
import json
import numpy as np
from orchestration.common import atomic_json,ManifestError,sha,fingerprint

SPECS={'sample_key':('S64',()),'as_of':('<i8',()),'length':('u1',()),'category':('<i4',(128,)),'behavior':('u1',(128,)),'hour':('u1',(128,)),'gap':('<f4',(128,)),'numeric':('<f4',(14,)),'label':('i1',()),'history_event_count':('<i8',()),'cohort':('u1',())}
class ShardWriter:
    def __init__(self,path,mean,std,capacity=32768):
        if not 1<=capacity<=32768:raise ValueError('shard capacity out of range')
        self.path=Path(path);self.path.mkdir(parents=True,exist_ok=False);self.mean=np.asarray(mean,dtype=np.float64);self.std=np.asarray(std,dtype=np.float64);self.capacity=capacity;self.buffers={};self.shards=[];self.count=0;self.positives=0;self.zeros=0;self.index=0;self.identities=hashlib.sha256();self.current=None
    def _open(self):
        self.current=self.path/f'shard-{len(self.shards):05}';self.current.mkdir()
        self.buffers={k:np.lib.format.open_memmap(self.current/(k+'.npy'),mode='w+',dtype=dtype,shape=(self.capacity,*shape)) for k,(dtype,shape) in SPECS.items()}
        self.ids=(self.current/'user_ids.utf8').open('wb');self.offsets=np.lib.format.open_memmap(self.current/'user_offsets.npy',mode='w+',dtype='<i8',shape=(self.capacity+1,));self.offsets[0]=0;self.index=0
    def add(self,row):
        if self.current is None:self._open()
        n=self.index;length=int(row['length']);assert 1<=length<=128
        for k in SPECS:
            if k=='numeric':v=(np.asarray(row[k],dtype=np.float64)-self.mean)/self.std
            elif k=='cohort':v=1 if row[k]=='SEEN_TRAIN' else 0
            else:v=row[k]
            self.buffers[k][n]=v
        raw=row['user_id'].encode();self.ids.write(raw);self.offsets[n+1]=self.offsets[n]+len(raw)
        self.identities.update((row['sample_key']+'\n').encode());self.count+=1;self.positives+=row['label']==1;self.zeros+=row['label']==0;self.index+=1
        if self.index==self.capacity:self._close()
    def _close(self):
        n=self.index;self.ids.close()
        # 尾分片也以实际shape发布；一次只重写一列，内存有界。
        for k,arr in list(self.buffers.items()):
            arr.flush()
            if n<self.capacity:
                target=self.current/(k+'.trim.npy');np.save(target,np.asarray(arr[:n]));del self.buffers[k];del arr
                target.replace(self.current/(k+'.npy'))
        self.offsets.flush()
        if n<self.capacity:
            target=self.current/'offsets.trim.npy';np.save(target,np.asarray(self.offsets[:n+1]));del self.offsets;target.replace(self.current/'user_offsets.npy')
        else:del self.offsets
        self.buffers.clear()
        files=[{'path':str(p.relative_to(self.path)),'sha256':sha(p),'bytes':p.stat().st_size} for p in sorted(self.current.iterdir())]
        self.shards.append({'directory':self.current.name,'rows':n,'files':files});self.current=None
    def finish(self):
        if self.current:self._close()
        m={'schema_version':'1.0','rows':self.count,'positive_count':self.positives,'negative_count':self.zeros,'sample_order_sha256':self.identities.hexdigest(),'shards':self.shards}
        atomic_json(self.path/'manifest.json',m);return m

class ShardDataset:
    """逐分片mmap；顺序和片内排列确定，不对全量样本构建Python列表。"""
    def __init__(self,path,verify=True):
        self.path=Path(path);self.manifest=json.loads((self.path/'manifest.json').read_text())
        if verify:
            for shard in self.manifest['shards']:
                for f in shard['files']:
                    p=self.path/f['path']
                    if sha(p)!=f['sha256'] or p.stat().st_size!=f['bytes']:raise ManifestError('Sample shard changed')
    def batches(self,batch_size=256,seed=42,epoch=0,shuffle=True):
        if batch_size<1:raise ValueError('batch_size must be positive')
        rng=np.random.default_rng(seed+epoch);shards=list(self.manifest['shards'])
        if shuffle:rng.shuffle(shards)
        for shard in shards:
            p=self.path/shard['directory'];arrays={k:np.load(p/(k+'.npy'),mmap_mode='r',allow_pickle=False) for k in SPECS}
            order=rng.permutation(shard['rows']) if shuffle else np.arange(shard['rows'])
            for start in range(0,len(order),batch_size):
                indices=order[start:start+batch_size]
                yield {k:np.array(a[indices],copy=True) for k,a in arrays.items()}
