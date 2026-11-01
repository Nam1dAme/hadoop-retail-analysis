import json
from pathlib import Path
import uuid
from .common import *
from .storage import Hdfs
from .pipeline import get_stage,files,stage_run
from ml.shards import ShardWriter

def prepare(lease,batch):
    storage=Hdfs(lease.config,lease);raw=get_stage(lease,batch,'J05');moments=get_stage(lease,batch,'J06')
    normalization=[json.loads(line.split('\t',1)[1]) for line in storage.dump(files(moments))]
    if len(normalization)!=1:raise ManifestError('Expected one training normalization')
    normalization=normalization[0]
    if len(normalization['mean'])!=14 or len(normalization['std'])!=14 or any(v<=0 for v in normalization['std']):raise ManifestError('Invalid normalization')
    from .audit import populations
    users=get_stage(lease,batch,'J04users')
    expected=populations(storage,users)['samples']
    clean=get_stage(lease,batch,'J01')
    observed=clean['counters'].get('DAILY',{})
    from datetime import date,timedelta
    days=[str(date(2017,11,25)+timedelta(days=d)) for d in range(9)]
    missing=[d for d in days if not observed.get(d)]
    if missing and not lease.config.get('synthetic'):raise QualityError('Missing observation dates; cannot label missing days as negatives')
    coverage={'business_timezone':'Asia/Shanghai','observed_days':observed,'missing_days':missing,'source_provenance_pending':True,'scope':'recorded behaviors; local research only'}
    def export(storage,temp,local,run_id):
        cache=Path(lease.config['paths']['work_dir'])/'sample-cache'/run_id
        writers={s:ShardWriter(cache/s,normalization['mean'],normalization['std']) for s in ('TRAIN','VALIDATION','TEST','FORECAST')}
        for line in storage.dump(files(raw)):
            key,value=line.split('\t',1);row=json.loads(value)
            if key!=row['sample_key']:raise ManifestError('Sample identity mismatch')
            writers[row['split']].add(row)
        summaries={s:w.finish() for s,w in writers.items()}
        from ml.shards import ShardDataset
        for s in summaries:ShardDataset(cache/s,verify=True)
        for s,m in summaries.items():
            expected_rows=raw['counters']['SAMPLES'].get(s,0)
            if expected_rows!=m['rows']:raise ManifestError('Export rows differ from Hadoop counters')
            if m['seen_count']!=expected[s]['seen'] or m['unseen_count']!=expected[s]['unseen']:raise ManifestError('Cohort population differs from TRAIN-only bitmap membership')
            if any(m[k]!=expected[s][k] for k in ('rows','positive_count','negative_count')):raise ManifestError('Sample population or label counts differ from independent user-day masks')
        if normalization['count']!=summaries['TRAIN']['rows']:raise ManifestError('Normalization population differs')
        for s in ('TRAIN','VALIDATION','TEST'):
            m=summaries[s]
            if not lease.config.get('synthetic') and (m['rows']<1000 or min(m['positive_count'],m['negative_count'])<50):raise QualityError('Prediction sample population gate failed')
        atomic_json(cache/'normalization.json',normalization)
        atomic_json(cache/'manifest.json',{'schema_version':'1.0','split_version':'daily-48h-24h-v1','batch_id':batch,'run_id':run_id,'splits':summaries,'normalization':normalization,'coverage':coverage})
        storage.put(cache,temp+'/shards')
        return {'samples':{'cache_path':str(cache),'splits':{s:{k:m[k] for k in ('rows','positive_count','negative_count','seen_count','unseen_count','sample_order_sha256')} for s,m in summaries.items()},'normalization':normalization,'coverage':coverage,'expected_population':expected,'archive_path':temp+'/shards'}}
    manifest=stage_run(lease,batch,'E01',files(raw)+files(moments),{'split_version':'daily-48h-24h-v1','shard_capacity':32768,'coverage':coverage,'population_audit':expected},export)
    info=manifest['samples']
    cache=Path(info['cache_path'])
    if not cache.exists():
        cache.parent.mkdir(parents=True,exist_ok=True)
        storage.command('-get',info['archive_path'],str(cache))
    for f in manifest['outputs']:
        prefix=info['archive_path']+'/'
        if f['path'].startswith(prefix):
            local_file=cache/f['path'][len(prefix):]
            if not local_file.is_file() or sha(local_file)!=f['sha256']:raise ManifestError('Cached sample artifact changed')
    from ml.shards import ShardDataset
    for split in ('TRAIN','VALIDATION','TEST','FORECAST'):
        ShardDataset(cache/split,verify=True)
    sample_hash=fingerprint(manifest['outputs']);norm_hash=fingerprint(info['normalization'])
    with lease.db.cursor() as q:
        q.execute('SELECT experiment_id FROM experiment WHERE batch_id=%s AND sample_hash=%s',(batch,sample_hash));row=q.fetchone()
        experiment=row['experiment_id'] if row else str(uuid.uuid4())
        if not row:q.execute('INSERT INTO experiment VALUES(%s,%s,%s,%s,%s,%s,%s,NULL,NULL,%s,%s,%s)',(experiment,batch,'daily-48h-24h-v1',str(Path(manifest['outputs'][0]['path']).parent/'manifest.json'),sample_hash,norm_hash,'PENDING','{}',now(),now()))
    return {'status':'SUCCEEDED','batch_id':batch,'run_id':manifest['run_id'],'experiment_id':experiment,'samples':info}
