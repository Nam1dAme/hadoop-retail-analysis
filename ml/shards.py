from pathlib import Path
import hashlib
import json
import numpy as np
from orchestration.common import atomic_json,ManifestError,sha,fingerprint

SPECS={'sample_key':('S64',()),'as_of':('<i8',()),'length':('u1',()),'category':('<i4',(128,)),'behavior':('u1',(128,)),'hour':('u1',(128,)),'gap':('<f4',(128,)),'numeric':('<f4',(14,)),'label':('i1',()),'history_event_count':('<i8',()),'cohort':('u1',())}
class ShardWriter:
    def __init__(self,path,mean,std,capacity=32768):
        if not 1<=capacity<=32768:raise ValueError('shard capacity out of range')
        self.path=Path(path);self.path.mkdir(parents=True,exist_ok=False);self.mean=np.asarray(mean,dtype=np.float64);self.std=np.asarray(std,dtype=np.float64);self.capacity=capacity;self.buffers={};self.shards=[];self.count=0;self.positives=0;self.zeros=0;self.seen=0;self.index=0;self.identities=hashlib.sha256();self.current=None
    def _open(self):
        self.current=self.path/f'shard-{len(self.shards):05}';self.current.mkdir()
        self.buffers={k:np.lib.format.open_memmap(self.current/(k+'.npy'),mode='w+',dtype=dtype,shape=(self.capacity,*shape)) for k,(dtype,shape) in SPECS.items()}
        self.ids=(self.current/'user_ids.utf8').open('wb');self.offsets=np.lib.format.open_memmap(self.current/'user_offsets.npy',mode='w+',dtype='<i8',shape=(self.capacity+1,));self.offsets[0]=0;self.index=0
    def add(self,row):
        if self.current is None:self._open()
        n=self.index;length=int(row['length']);assert 1<=length<=128
        if row['cohort'] not in ('SEEN_TRAIN','UNSEEN_TRAIN'):raise ManifestError('Unknown cohort')
        for k in SPECS:
            if k=='numeric':v=(np.asarray(row[k],dtype=np.float64)-self.mean)/self.std
            elif k=='cohort':v=1 if row[k]=='SEEN_TRAIN' else 0
            else:v=row[k]
            self.buffers[k][n]=v
        raw=row['user_id'].encode();self.ids.write(raw);self.offsets[n+1]=self.offsets[n]+len(raw)
        self.identities.update((row['sample_key']+'\n').encode());self.count+=1;self.positives+=row['label']==1;self.zeros+=row['label']==0;self.seen+=row['cohort']=='SEEN_TRAIN';self.index+=1
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
        m={'schema_version':'1.0','rows':self.count,'positive_count':self.positives,'negative_count':self.zeros,'seen_count':self.seen,'unseen_count':self.count-self.seen,'sample_order_sha256':self.identities.hexdigest(),'shards':self.shards}
        atomic_json(self.path/'manifest.json',m);return m

class ShardDataset:
    """逐分片mmap；顺序和片内排列确定，不对全量样本构建Python列表。"""
    def __init__(self,path,verify=True):
        self.path=Path(path);self.manifest=json.loads((self.path/'manifest.json').read_text())
        if self.manifest.get('schema_version')!='1.0':raise ManifestError('Unknown sample schema')
        if verify:
            count=positive=negative=seen=0
            for shard in self.manifest['shards']:
                for f in shard['files']:
                    p=self.path/f['path']
                    if not p.resolve().is_relative_to(self.path.resolve()):raise ManifestError('Unsafe shard file reference')
                    if sha(p)!=f['sha256'] or p.stat().st_size!=f['bytes']:raise ManifestError('Sample shard changed')
                p=self.path/shard['directory'];n=shard['rows'];count+=n
                if not 1<=n<=32768:raise ManifestError('Invalid shard rows')
                arrays={k:np.load(p/(k+'.npy'),mmap_mode='r',allow_pickle=False) for k in SPECS}
                for k,(dtype,shape) in SPECS.items():
                    if arrays[k].shape!=(n,*shape) or arrays[k].dtype!=np.dtype(dtype):raise ManifestError('Shard shape/dtype mismatch: '+k)
                lengths=arrays['length']
                if np.any((lengths<1)|(lengths>128)):raise ManifestError('Invalid sequence lengths')
                padding=np.arange(128)[None,:]>=lengths[:,None]
                for k,maximum in [('category',16384),('behavior',4),('hour',24)]:
                    x=arrays[k]
                    if np.any(x[padding]!=0) or np.any(x[~padding]<1) or np.any(x>maximum):raise ManifestError('Invalid sequence/padding: '+k)
                if not np.isin(arrays['cohort'],[0,1]).all() or np.any(arrays['history_event_count']<lengths):raise ManifestError('Invalid sample population metadata')
                if not np.isfinite(arrays['numeric']).all() or not np.isfinite(arrays['gap']).all():raise ManifestError('Non-finite feature')
                if np.any(arrays['gap'][padding]!=0) or np.any((arrays['gap']<0)|(arrays['gap']>1)) or np.any(arrays['gap'][:,0]!=0):raise ManifestError('Invalid time gaps')
                labels=arrays['label']
                if not np.isin(labels,[-1] if self.path.name=='FORECAST' else [0,1]).all():raise ManifestError('Invalid labels')
                positive+=int(np.count_nonzero(labels==1));negative+=int(np.count_nonzero(labels==0));seen+=int(np.count_nonzero(arrays['cohort']==1))
                offsets=np.load(p/'user_offsets.npy',mmap_mode='r',allow_pickle=False)
                if offsets.dtype!=np.dtype('<i8') or offsets.shape!=(n+1,) or offsets[0]!=0 or np.any(np.diff(offsets)<1) or offsets[-1]!=(p/'user_ids.utf8').stat().st_size:raise ManifestError('Invalid ID offsets')
                raw=(p/'user_ids.utf8').read_bytes()
                for i in range(n):
                    user=raw[offsets[i]:offsets[i+1]]
                    if not 1<=len(user)<=20 or not user.isdigit():raise ManifestError('Invalid source user ID')
                    expected=hashlib.sha256(user+b'|'+str(int(arrays['as_of'][i])).encode()).hexdigest().encode()
                    if bytes(arrays['sample_key'][i])!=expected:raise ManifestError('Sample key does not match source user and anchor')
            if 'seen_count' in self.manifest and (seen,count-seen)!=(self.manifest['seen_count'],self.manifest['unseen_count']):raise ManifestError('Cohort counts differ')
            if (count,positive,negative)!=(self.manifest['rows'],self.manifest['positive_count'],self.manifest['negative_count']):raise ManifestError('Shard population differs')
    def batches(self,batch_size=256,seed=42,epoch=0,shuffle=True,columns=None):
        if batch_size<1:raise ValueError('batch_size must be positive')
        rng=np.random.default_rng(seed+epoch);shards=list(self.manifest['shards'])
        if shuffle:rng.shuffle(shards)
        for shard in shards:
            p=self.path/shard['directory'];arrays={k:np.load(p/(k+'.npy'),mmap_mode='r',allow_pickle=False) for k in (SPECS if columns is None else columns)}
            order=rng.permutation(shard['rows']) if shuffle else np.arange(shard['rows'])
            for start in range(0,len(order),batch_size):
                indices=order[start:start+batch_size]
                yield {k:np.array(a[indices],copy=True) for k,a in arrays.items()}

def training_groups(dataset,micro_batch=256,effective_batch=512,seed=42,epoch=0,columns=None):
    """跨分片累计到有效batch；返回按真实样本数归一的每个microbatch权重。"""
    if dataset.path.name!='TRAIN':raise ValueError('Training groups require TRAIN samples')
    if micro_batch<1 or effective_batch<1 or micro_batch>effective_batch:raise ValueError('Invalid batch sizes')
    group=[];total=0
    for batch in dataset.batches(micro_batch,seed,epoch,shuffle=True,columns=columns):
        offset=0;n=len(batch['label'])
        while offset<n:
            take=min(n-offset,effective_batch-total)
            group.append({k:v[offset:offset+take] for k,v in batch.items()});total+=take;offset+=take
            if total==effective_batch:
                yield group,[len(x['label'])/total for x in group],total
                group=[];total=0
    if group:yield group,[len(x['label'])/total for x in group],total
