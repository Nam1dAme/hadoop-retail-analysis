"""Bounded idempotent loaders; incomplete versions remain invisible to API readers."""
from datetime import datetime,timedelta,timezone
import itertools
import json
import os
from pathlib import Path
import time
import uuid
import numpy as np
from orchestration.common import ROOT,atomic_json,sha,fingerprint,encode,now,stamp,ManifestError,PipelineError
from orchestration.pipeline import latest,files,validate
from orchestration.storage import Hdfs

TABLE_WIDTHS={'event_cube':5,'entity_daily':6,'user_daily':10,'range_summary':7,'activity_daily':5,'prediction':11}


def load_rows(lease,release_id,dataset,table,source_hash,rows):
    if table not in TABLE_WIDTHS:
        raise ValueError('Unrecognized result dataset')
    with lease.db.cursor() as q:
        q.execute('SELECT * FROM release_load WHERE release_id=%s AND dataset=%s',(release_id,dataset));previous=q.fetchone()
        if previous and previous['source_hash']!=source_hash:
            raise ManifestError('Load source changed for same release')
        if previous and previous['complete']:
            return previous['rows_loaded']
    offset=int(previous['rows_loaded']) if previous else 0
    stream=iter(rows)
    for _ in range(offset):
        try:next(stream)
        except StopIteration:raise ManifestError('Resumed source shorter than committed load') from None
    count=offset
    sql='INSERT INTO '+table+' VALUES('+','.join(['%s']*TABLE_WIDTHS[table])+')'
    while True:
        lease.guard();batch=list(itertools.islice(stream,1000))
        if not batch:break
        if any(len(row)!=TABLE_WIDTHS[table] or row[0]!=release_id for row in batch):
            raise ManifestError('Result row identity/shape differs')
        if sum(len(encode(row).encode()) for row in batch)>8*1024**2:
            raise ManifestError('Result transaction exceeds 8 MiB')
        lease.db.begin()
        try:
            with lease.db.cursor() as q:
                q.execute("SELECT status FROM data_release WHERE release_id=%s FOR UPDATE",(release_id,))
                if q.fetchone()['status']!='STAGING':raise ManifestError('Only staging releases may be loaded')
                q.executemany(sql,batch);count+=len(batch)
                q.execute('INSERT INTO release_load VALUES(%s,%s,%s,%s,0,%s) ON DUPLICATE KEY UPDATE rows_loaded=VALUES(rows_loaded),updated_at=VALUES(updated_at)',
                          (release_id,dataset,source_hash,count,now()))
            lease.db.commit()
        except BaseException:
            lease.db.rollback();raise
        if count%100000==0:
            print(encode(dict(dataset=dataset,rows=count,release_id=release_id)),flush=True)
        if os.environ.get('BEHAVIOR_TEST_STOP_LOAD')==dataset:
            raise PipelineError('Explicit staging load interruption test')
    with lease.db.cursor() as q:
        q.execute('INSERT INTO release_load VALUES(%s,%s,%s,%s,1,%s) ON DUPLICATE KEY UPDATE rows_loaded=VALUES(rows_loaded),complete=1,updated_at=VALUES(updated_at)',
                  (release_id,dataset,source_hash,count,now()))
    return count


def prediction_rows(release_id,mode,model_id,path,cache,threshold,lease):
    predictions=np.load(path,mmap_mode='r',allow_pickle=False)
    manifest=json.loads((cache/'manifest.json').read_text());offset=0
    for shard in manifest['shards']:
        lease.guard();directory=cache/shard['directory'];n=shard['rows']
        columns={k:np.load(directory/(k+'.npy'),mmap_mode='r',allow_pickle=False) for k in ('sample_key','as_of','history_event_count','label')}
        ids=(directory/'user_ids.utf8').read_bytes();positions=np.load(directory/'user_offsets.npy',mmap_mode='r',allow_pickle=False)
        for i in range(n):
            value=predictions[offset+i]
            if value['sample_key']!=columns['sample_key'][i] or value['label']!=columns['label'][i]:
                raise ManifestError('Prediction row differs from original sample')
            score=float(value['score'])
            if not np.isfinite(score) or not 0<=score<=1:
                raise ManifestError('Invalid prediction score')
            user=ids[positions[i]:positions[i+1]].decode('utf-8')
            as_of=datetime.fromtimestamp(int(columns['as_of'][i]),timezone.utc).replace(tzinfo=None)
            label=None if mode=='FORECAST' else int(value['label'])
            if (mode=='FORECAST' and value['label']!=-1) or (mode=='TEST' and label not in (0,1)):
                raise ManifestError('Invalid label visibility')
            # Datetimes are converted to SQL strings so batch-size accounting is serializable.
            yield (release_id,mode,model_id,user,as_of.isoformat(sep=' '),(as_of+timedelta(days=1)).isoformat(sep=' '),
                   score,score>=threshold,label,int(columns['history_event_count'][i]),bytes(value['sample_key']).decode())
        offset+=n
    if offset!=len(predictions):raise ManifestError('Prediction row count differs')


def publish_pointer(db,release_id,expected_generation,operator='behavior-cli',action='PUBLISH'):
    """Short transaction: status, current CAS and audit event are indivisible."""
    db.begin()
    try:
        with db.cursor() as q:
            q.execute('SELECT * FROM current_release WHERE singleton_id=1 FOR UPDATE');current=q.fetchone()
            q.execute('SELECT status FROM data_release WHERE release_id=%s FOR UPDATE',(release_id,));target=q.fetchone()
            if not target:raise ManifestError('Unknown release')
            if current['generation']!=expected_generation:raise ManifestError('Current generation changed; retry publication explicitly')
            if target['status']=='RETIRED':raise ManifestError('Retired release cannot be published or rolled back')
            if action=='ROLLBACK' and target['status']!='READY':raise ManifestError('Rollback requires an existing READY release')
            if action=='PUBLISH':
                q.execute('SELECT dataset,complete FROM release_load WHERE release_id=%s',(release_id,));parts=q.fetchall()
                expected={'event_cube','entity_daily','user_daily','range_summary','activity_daily','TEST-CONSTANT','TEST-LOGISTIC','TEST-GRU','FORECAST'}
                if {x['dataset'] for x in parts}!=expected or not all(x['complete'] for x in parts):raise ManifestError('Incomplete release datasets')
                q.execute("UPDATE data_release SET status='READY',published_at=COALESCE(published_at,%s) WHERE release_id=%s",(now(),release_id))
            q.execute('UPDATE current_release SET release_id=%s,generation=generation+1 WHERE singleton_id=1 AND generation=%s',(release_id,expected_generation))
            if q.rowcount!=1:raise ManifestError('Current generation CAS failed')
            q.execute('INSERT INTO release_event VALUES(%s,%s,%s,%s,%s,%s)',(str(uuid.uuid4()),release_id,action,current['release_id'],now(),operator))
        db.commit()
    except BaseException:
        db.rollback();raise


def publish(lease):
    work=ROOT/'var/production'
    models=json.loads((work/'models.json').read_text());forecast=json.loads((work/'forecast.json').read_text())
    samples=json.loads((work/'samples.json').read_text());pipeline=json.loads((work/'pipeline.json').read_text())
    if not(models['status']==forecast['status']==samples['status']==pipeline['status']=='SUCCEEDED'):
        raise ManifestError('Release requires all stages to succeed')
    if len({x['batch_id'] for x in (models,forecast,samples,pipeline)})!=1:
        raise ManifestError('Release dependencies cross batches')
    if forecast['forecast']['checkpoint_hash']!=next(x['checkpoint_hash'] for x in models['selection']['finalists'] if x['model_run_id']==models['selected_model_id']):
        raise ManifestError('Forecast weights differ from selected test checkpoint')
    storage=Hdfs(lease.config,lease);batch=models['batch_id']
    if storage.json(models['archive']+'/manifest.json')!=models:
        raise ManifestError('Model index differs from immutable archive')
    validate(storage,forecast['output_path'],{'run_id':forecast['run_id'],'batch_id':batch})
    with lease.db.cursor() as q:
        q.execute('SELECT status,selected_model_id,test_sealed_at FROM experiment WHERE experiment_id=%s AND batch_id=%s',(models['experiment_id'],batch));experiment=q.fetchone()
        if not experiment or experiment['status']!='SUCCEEDED' or experiment['test_sealed_at'] is None or experiment['selected_model_id']!=models['selected_model_id']:
            raise ManifestError('Completed sealed experiment is required')
        q.execute("SELECT COUNT(*) AS n FROM model_run WHERE experiment_id=%s AND algorithm='GRU' AND device='cuda' AND status='SUCCEEDED'",(models['experiment_id'],))
        if q.fetchone()['n']!=2:raise ManifestError('Both full CUDA candidates must complete')
        q.execute('SELECT device,COUNT(*) AS n FROM benchmark WHERE experiment_id=%s GROUP BY device',(models['experiment_id'],))
        if {r['device']:r['n'] for r in q.fetchall()}!={'cpu':3,'cuda':3}:raise ManifestError('Complete CPU/CUDA benchmark required')
    stages={}
    for name in ('J03','J04users','J04summary'):
        row=latest(lease.db,batch,name)
        if not row:raise ManifestError('Missing analysis stage')
        stages[name]=validate(storage,row['output_path'],{'run_id':row['run_id'],'batch_id':batch})
    manifest=dict(schema_version='release-v1',baseline_version='UBA-DOC-1.0.0',batch_id=batch,
                  experiment_id=models['experiment_id'],selected_model_id=models['selected_model_id'],
                  inputs={name:sha(work/(name+'.json')) for name in ('models','forecast','samples','pipeline')},
                  models_archive=models['archive'],forecast_archive=forecast['output_path'],
                  samples=samples['samples']['splits'],analysis=pipeline['analysis'],quality=pipeline['quality'],
                  stages={k:{'run_id':v['run_id'],'outputs':v['outputs']} for k,v in stages.items()})
    manifest_hash=fingerprint(manifest)
    with lease.db.cursor() as q:
        q.execute('SELECT * FROM data_release WHERE manifest_hash=%s',(manifest_hash,));release=q.fetchone()
        if release and release['status']=='READY':
            result=dict(status='SUCCEEDED',release_id=release['release_id'],manifest_hash=manifest_hash)
            atomic_json(work/'release.json',result);return result
        if not release:
            release_id=str(uuid.uuid4())
            remote=f'{lease.config["hdfs"]["root"]}/releases/{release_id}/manifest.json'
            q.execute('INSERT INTO data_release VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)',
                      (release_id,batch,models['experiment_id'],models['selected_model_id'],'STAGING',remote,manifest_hash,encode(manifest),
                       '2017-12-03 16:00:00','2017-12-04 16:00:00',now()))
        else:release_id=release['release_id'];remote=release['manifest_path']
        q.execute('SELECT generation FROM current_release WHERE singleton_id=1');generation=q.fetchone()['generation']
    started=time.monotonic();counts={}
    def analysis_rows(name):
        stage='J03' if name in ('event_cube','entity_daily') else 'J04users' if name=='user_daily' else 'J04summary'
        for line in storage.dump(files(stages[stage],name)):
            values=line.split('\t')
            if name=='event_cube':day,hour,kind,count=values;yield(release_id,day,int(hour),kind,int(count))
            elif name=='entity_daily':day,kind,entity,behavior,count=values;yield(release_id,day,kind,entity,behavior,int(count))
            elif name=='user_daily':user,day,*numbers=values;yield(release_id,user,day,*map(int,numbers))
            elif name=='range_summary':start,end,*numbers=values;yield(release_id,start,end,*map(int,numbers))
            else:
                day,active,first,returns=values;yield(release_id,day,int(active),int(first),None if int(returns)<0 else int(returns))
    for name in ('event_cube','entity_daily','user_daily','range_summary','activity_daily'):
        stage='J03' if name in ('event_cube','entity_daily') else 'J04users' if name=='user_daily' else 'J04summary'
        source_files=files(stages[stage],name)
        count=load_rows(lease,release_id,name,name,fingerprint(source_files),analysis_rows(name))
        expected=sum(f['rows'] for f in source_files)
        if count!=expected:raise ManifestError('Loaded analysis row count differs: '+name)
        counts[name]=count
    cache=Path(samples['samples']['cache_path'])
    for item in models['test']:
        path=Path(item['scores'])
        if sha(path)!=item['sha256']:raise ManifestError('Sealed test scores changed')
        dataset='TEST-'+item['algorithm']
        counts[dataset]=load_rows(lease,release_id,dataset,'prediction',item['sha256'],
                                 prediction_rows(release_id,'TEST',item['model_run_id'],path,cache/'TEST',item['threshold'],lease))
        if counts[dataset]!=samples['samples']['splits']['TEST']['rows']:raise ManifestError('Test population differs')
    info=forecast['forecast'];path=Path(info['scores']['path'])
    if sha(path)!=info['scores']['sha256']:raise ManifestError('Forecast scores changed')
    counts['FORECAST']=load_rows(lease,release_id,'FORECAST','prediction',info['scores']['sha256'],
                                prediction_rows(release_id,'FORECAST',info['model_run_id'],path,cache/'FORECAST',info['threshold'],lease))
    if counts['FORECAST']!=samples['samples']['splits']['FORECAST']['rows']:raise ManifestError('Forecast population differs')
    with lease.db.cursor() as q:
        for table in ('event_cube','entity_daily','user_daily','range_summary','activity_daily'):
            q.execute('SELECT COUNT(*) AS n FROM '+table+' WHERE release_id=%s',(release_id,))
            if q.fetchone()['n']!=counts[table]:raise ManifestError('Persisted row count differs')
        q.execute('SELECT SUM(event_count) AS n FROM event_cube WHERE release_id=%s',(release_id,))
        if int(q.fetchone()['n'])!=pipeline['analysis']['scoped_rows']:raise ManifestError('Loaded cube conservation failed')
        q.execute('SELECT SUM(event_count) AS n FROM entity_daily WHERE release_id=%s',(release_id,))
        if int(q.fetchone()['n'])!=2*pipeline['analysis']['scoped_rows']:raise ManifestError('Loaded entity conservation failed')
        q.execute('SELECT COUNT(*) AS n FROM prediction WHERE release_id=%s',(release_id,))
        if q.fetchone()['n']!=sum(counts[k] for k in counts if k.startswith('TEST-') or k=='FORECAST'):raise ManifestError('Loaded prediction count differs')
        q.execute("SELECT COUNT(*) AS n FROM prediction WHERE release_id=%s AND mode='FORECAST' AND label IS NOT NULL",(release_id,))
        if q.fetchone()['n']!=0:raise ManifestError('Unobserved labels became known')
    manifest_file=work/'release-manifest.json';atomic_json(manifest_file,manifest)
    storage.mkdir(str(Path(remote).parent))
    if not storage.exists(remote):storage.put(manifest_file,remote)
    elif storage.json(remote)!=manifest:raise ManifestError('Immutable release manifest differs')
    with lease.db.cursor() as q:q.execute('UPDATE data_release SET loaded_at=%s WHERE release_id=%s',(now(),release_id))
    lease.guard();publish_pointer(lease.db,release_id,generation)
    result=dict(status='SUCCEEDED',release_id=release_id,manifest_hash=manifest_hash,manifest_path=remote,
                counts=counts,seconds=time.monotonic()-started,published_at=stamp())
    atomic_json(work/'release.json',result)
    return result
