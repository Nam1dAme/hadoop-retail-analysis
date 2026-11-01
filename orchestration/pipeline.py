import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from .common import *
from .control import alive,live_java
from .storage import Hdfs,JAR,run_job,application,discover

STAGES=('J01','J02','J03','J03rank','J04users','J04summary','J05','J06')

def validate(storage,path,expected=None):
    manifest=storage.json(path+'/manifest.json')
    if expected:
        for k,v in expected.items():
            if manifest.get(k)!=v:raise ManifestError('Manifest identity mismatch: '+k)
    if manifest['baseline_version']!=BASELINE or manifest['status']!='SUCCEEDED':raise ManifestError('Invalid manifest version or status')
    if fingerprint(manifest['config'])!=manifest['config_sha256'] or fingerprint(manifest['inputs'])!=manifest['input_hash']:raise ManifestError('Manifest parameter/input fingerprint differs')
    if manifest['stage']=='I00' and storage.digest(manifest['source']['path'])!=(manifest['source']['sha256'],manifest['source']['bytes']):raise ManifestError('Raw archive changed')
    actual=[x for x in storage.inspect(path) if not x['path'].endswith('/manifest.json')]
    if actual!=manifest['outputs']:raise ManifestError('Immutable output hash/row/schema changed')
    return manifest


def latest(db,batch,stage):
    with db.cursor() as q:
        q.execute("SELECT * FROM task_run WHERE batch_id=%s AND stage=%s AND status='SUCCEEDED' ORDER BY finished_at DESC LIMIT 1",(batch,stage));return q.fetchone()


def stage_run(lease,batch,stage,inputs,parameters,executor=None):
    lease.guard();db=lease.db;storage=Hdfs(lease.config,lease)
    env_hash=sha(ROOT/'config/environment.lock.json');code=commit()
    params={'stage':stage,'jar_sha256':sha(JAR),'parameters':parameters,'baseline_version':BASELINE,'runtime_config_sha256':fingerprint(lease.config)}
    input_hash=fingerprint(inputs);config_hash=fingerprint(params)
    identity={'input_hash':input_hash,'config_sha256':config_hash,'code_commit':code,'environment_sha256':env_hash,'batch_id':batch,'stage':stage}
    with db.cursor() as q:
        q.execute('SELECT * FROM task_run WHERE batch_id=%s AND stage=%s AND input_hash=%s AND config_hash=%s AND code_commit=%s AND environment_hash=%s ORDER BY attempt DESC',(batch,stage,input_hash,config_hash,code,env_hash));previous=q.fetchall()
    for row in previous:
        if row['status']=='SUCCEEDED':return validate(storage,row['output_path'],identity)
    # 即便参数改变，也不能覆盖或遗忘尚在运行的尝试。
    with db.cursor() as q:
        q.execute("SELECT * FROM task_run WHERE status IN ('PENDING','RUNNING')");unfinished=q.fetchall()
    for row in unfinished:
        if alive(row) or live_java(row['run_id']):raise LockBusy('Original process is still alive: '+row['run_id'])
        apps=discover(lease.config,'behavior-'+row['stage']+'-'+row['run_id'])
        app=application(lease.config,row['application_id']) if row['application_id'] else None
        if any(x['state'] not in ('FINISHED','FAILED','KILLED') for x in apps+([app] if app else [])):raise LockBusy('Original YARN application is active')
        if storage.exists(row['output_path']+'/manifest.json'):
            recovered=validate(storage,row['output_path'],{'run_id':row['run_id'],'batch_id':row['batch_id'],'code_commit':row['code_commit'],'config_sha256':row['config_hash'],'environment_sha256':row['environment_hash']})
            with db.cursor() as q:q.execute("UPDATE task_run SET status='SUCCEEDED',finished_at=%s,updated_at=%s WHERE run_id=%s",(now(),now(),row['run_id']))
            if row['run_id'] in {x['run_id'] for x in previous}:return recovered
        else:
            with db.cursor() as q:q.execute("UPDATE task_run SET status='FAILED',error_code='ABANDONED_ATTEMPT',finished_at=%s,updated_at=%s WHERE run_id=%s",(now(),now(),row['run_id']))
    run_id=str(uuid.uuid4());attempt=previous[0]['attempt']+1 if previous else 1
    parent=previous[0]['run_id'] if previous else None
    root=lease.config['hdfs']['root'];branch='facts' if stage=='J01' else 'samples' if stage in ('J05','J06','E01') else 'reports' if stage in ('I00','J02','Q01','A01') else 'aggregates'
    final=f'{root}/{branch}/batch_id={batch}/{stage}/run_id={run_id}'
    temporary=str(Path(final).parent/('.'+run_id+'.tmp'))
    local=Path(lease.config['paths']['work_dir'])/'runs'/run_id;local.mkdir(parents=True,exist_ok=False)
    with db.cursor() as q:
        q.execute('INSERT INTO task_run(run_id,batch_id,stage,attempt,parent_run_id,status,input_hash,config_hash,code_commit,environment_hash,pid,process_started_at,lease_generation,heartbeat_at,started_at,manifest_path,parameters,output_path,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(run_id,batch,stage,attempt,parent,'RUNNING',input_hash,config_hash,code,env_hash,os.getpid(),process_start(os.getpid()),lease.generation,now(),now(),final+'/manifest.json',encode(params),final,now(),now()))
    lease.task=run_id
    def update_app(app_id):
        lease.guard()
        with db.cursor() as q:q.execute('UPDATE task_run SET application_id=%s,updated_at=%s WHERE run_id=%s AND lease_generation=%s',(app_id,now(),run_id,lease.generation))
    published=False
    try:
        storage.mkdir(str(Path(final).parent))
        if executor:
            storage.mkdir(temporary);extra=executor(storage,temporary,local,run_id) or {};app_id=None
        else:
            app_id=run_job(stage,inputs,temporary,dict(parameters,run_id=run_id),lease,local/'driver.log',update_app)
            metrics=storage.json(temporary+'/_metrics.json')
            if metrics['run_id']!=run_id or metrics['stage']!=stage:raise ManifestError('Job identity differs')
            if app_id!=metrics['application_id']:raise ManifestError('Application registration mismatch')
            app=application(lease.config,app_id)
            if not app or app['state']!='FINISHED' or app['finalStatus']!='SUCCEEDED':raise ManifestError('YARN application is not authoritatively successful')
            extra={'counters':metrics['counters']}
        storage.write_json(temporary+'/config.json',params,local/'config.json')
        storage.put(ROOT/'config/environment.lock.json',temporary+'/environment.lock.json')
        if not storage.exists(temporary+'/_SUCCESS'):
            (local/'_SUCCESS').touch();storage.put(local/'_SUCCESS',temporary+'/_SUCCESS')
        outputs=storage.inspect(temporary)
        for f in outputs:f['path']=f['path'].replace(temporary+'/',final+'/',1)
        def relocate(value):
            if isinstance(value,str):return value.replace(temporary+'/',final+'/',1) if value.startswith(temporary+'/') else value
            if isinstance(value,dict):return {k:relocate(v) for k,v in value.items()}
            if isinstance(value,list):return [relocate(v) for v in value]
            return value
        extra=relocate(extra)
        manifest={'output_path':final,'baseline_version':BASELINE,'schema_version':'1.0','policy_version':POLICY,'run_id':run_id,**identity,'inputs':inputs,'outputs':outputs,'config':params,'created_at':stamp(),'application_id':app_id,'synthetic':lease.config.get('synthetic',False),'sampled':False,'status':'SUCCEEDED',**extra}
        storage.write_json(temporary+'/manifest.json',manifest,local/'manifest.json');lease.guard();storage.rename(temporary,final);published=True
        if os.environ.get('BEHAVIOR_TEST_CRASH_AFTER_RENAME')==stage and lease.config.get('synthetic',False):os._exit(86)
        with db.cursor() as q:
            lease.guard();q.execute("UPDATE task_run SET status='SUCCEEDED',finished_at=%s,updated_at=%s WHERE run_id=%s AND lease_generation=%s",(now(),now(),run_id,lease.generation))
        return manifest
    except BaseException as error:
        if not published:
            try:
                lease.guard()
                with db.cursor() as q:q.execute("UPDATE task_run SET status='FAILED',error_code=%s,finished_at=%s,updated_at=%s WHERE run_id=%s",(getattr(error,'code','TASK_FAILED'),now(),now(),run_id))
            except Exception:pass
        raise
    finally:lease.task=None

def files(manifest,dataset=None):
    return [f for f in manifest['outputs'] if 'key_class' in f and (dataset is None or '/'+dataset+'/' in f['path'])]

def get_stage(lease,batch,stage):
    row=latest(lease.db,batch,stage)
    if not row:raise ConfigError('Missing successful prerequisite: '+stage)
    if not row['output_path'].startswith(lease.config['hdfs']['root']+'/'):raise ConfigError('Batch belongs to another HDFS namespace')
    return validate(Hdfs(lease.config,lease),row['output_path'],{'run_id':row['run_id'],'batch_id':batch})


def apply_quality(lease,batch,clean,profile=None):
    counters=clean['counters'].get('BEHAVIOR',{});names=('raw_rows','accepted_rows','rejected_rows','in_scope_rows','outside_rows');n={k:counters.get(k,0) for k in names}
    if n['raw_rows']!=n['accepted_rows']+n['rejected_rows'] or n['accepted_rows']!=n['in_scope_rows']+n['outside_rows'] or n['in_scope_rows']!=sum(counters.get('type_'+t,0) for t in ('pv','buy','cart','fav')):raise ManifestError('Quality count conservation failed')
    with lease.db.cursor() as q:
        q.execute('SELECT raw_rows FROM data_batch WHERE batch_id=%s',(batch,));expected=q.fetchone()['raw_rows']
        if expected!=n['raw_rows']:raise ManifestError('Source row count differs from Hadoop RecordReader')
    passed=n['raw_rows']>0 and n['accepted_rows']>0 and n['rejected_rows']*1000<=n['raw_rows'] and n['outside_rows']*100<=n['accepted_rows']
    data={'rows':n,'gate_passed':passed,'profile':{k:0 for k in ('user_count_all','item_count_all','category_count_all','duplicate_groups','duplicate_extra','multi_category_items','occupied_category_buckets','collision_buckets','colliding_categories')}}
    for group in ('BEHAVIOR','REJECTIONS','DAILY'):
        with lease.db.cursor() as q:
            for name,value in clean['counters'].get(group,{}).items():q.execute('INSERT INTO quality_metric VALUES(%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value),detail_json=VALUES(detail_json)',(batch,name,group,value,'{}'))
    if profile:
        for line in Hdfs(lease.config,lease).dump(files(profile)):
            key,value=line.split('\t');data['profile'][key]=data['profile'].get(key,0)+int(value)
        if profile['counters'].get('PROFILE_INPUT',{}).get('rows',0)!=n['accepted_rows']:raise ManifestError('Profile input differs from facts')
        with lease.db.cursor() as q:
            for key,value in data['profile'].items():q.execute('INSERT INTO quality_metric VALUES(%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value)',(batch,key,'PROFILE',value,'{}'))
    with lease.db.cursor() as q:
        q.execute('UPDATE data_batch SET accepted_rows=%s,rejected_rows=%s,in_scope_rows=%s,outside_rows=%s,status=%s,updated_at=%s WHERE batch_id=%s',(n['accepted_rows'],n['rejected_rows'],n['in_scope_rows'],n['outside_rows'],'SUCCEEDED' if passed and profile else 'RUNNING' if passed else 'FAILED',now(),batch))
    atomic_json(Path(lease.config['paths']['work_dir'])/'batches'/batch/'quality.json',data)
    def quality_bundle(storage,temp,local,run_id):
        storage.write_json(temp+'/quality.json',data,local/'quality.json');return {'quality':data}
    report=stage_run(lease,batch,'Q01',files(clean)+files(profile) if profile else files(clean),{'rejected_ratio_limit':0.001,'outside_ratio_limit':0.01},quality_bundle)
    with lease.db.cursor() as q:q.execute('UPDATE data_batch SET manifest_path=%s WHERE batch_id=%s',(report['output_path']+'/manifest.json',batch))
    return data


def pipeline(lease,batch,reducers=2,through='J04summary',audit=False,split_bytes=None):
    if not 1<=reducers<=64 or through not in STAGES:raise ConfigError('Invalid stage or reducers')
    source=get_stage(lease,batch,'I00')['source'];params={'reducers':reducers,'source_sha256':source['sha256']}
    if split_bytes:
        if not lease.config.get('synthetic'):raise ConfigError('Tiny split override is only for isolated synthetic tests')
        params['split_bytes']=split_bytes
    results={};quality=None
    for stage in STAGES[:STAGES.index(through)+1]:
        if stage=='J01':inputs=[source]
        elif stage=='J02':inputs=files(results['J01'],'facts')
        elif stage=='J03rank':inputs=files(results['J03'],'entity_daily')
        elif stage=='J04summary':inputs=files(results['J04users'],'partials')
        elif stage=='J06':inputs=files(results['J05'])
        else:inputs=files(results['J01'],'facts')
        results[stage]=stage_run(lease,batch,stage,inputs,params)
        if stage=='J02':
            quality=apply_quality(lease,batch,results['J01'],results['J02'])
            if not quality['gate_passed'] and not (audit and lease.config.get('synthetic')):raise QualityError('Quality thresholds failed; profile preserved')
    analysis_report=None
    if 'J04summary' in results:
        from .audit import analysis
        analysis_report=analysis(lease,batch,results)
    return {'analysis':analysis_report,'batch_id':batch,'stages':{k:{'run_id':v['run_id'],'application_id':v['application_id']} for k,v in results.items()},'quality':quality,'status':'SUCCEEDED'}
