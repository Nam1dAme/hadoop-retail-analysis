"""Conservative reference protection. Produces plans; never performs deletion."""
from datetime import timedelta
import json
from pathlib import PurePosixPath
from orchestration.common import ROOT,now,stamp,ManifestError


def protected_path(path,roots):
    p=PurePosixPath(path)
    if not p.is_absolute() or '..' in p.parts:raise ManifestError('Unsafe retention path')
    return any(p==PurePosixPath(root) or PurePosixPath(root) in p.parents for root in roots)


def plan(db,config):
    protected={config['source']['path']};candidates=[];blocked=[]
    with db.cursor() as q:
        q.execute('SELECT release_id FROM current_release WHERE singleton_id=1');current=q.fetchone()['release_id']
        # Keep every READY/STAGING version, exceeding the minimum of current + previous READY.
        q.execute("SELECT release_id,batch_id,experiment_id,manifest_json FROM data_release WHERE status IN ('READY','STAGING')")
        releases=q.fetchall();batches={r['batch_id'] for r in releases}
        for r in releases:
            manifest=json.loads(r['manifest_json'])
            for key in ('models_archive','forecast_archive'):
                if manifest.get(key):protected.add(manifest[key])
            protected.add(str(ROOT/'var/production/models'/r['experiment_id']))
        q.execute('SELECT run_id,batch_id,status,output_path,finished_at FROM task_run');tasks=q.fetchall()
        for task in tasks:
            if task['status'] in ('RUNNING','PENDING') or (task['status']=='SUCCEEDED' and task['batch_id'] in batches):
                protected.add(task['output_path'])
                protected.add(str(ROOT/'var/production/runs'/task['run_id']))
        # The unique source archive is always retained, even after a local-only backup.
        q.execute('SELECT batch_id,manifest_path,source_sha256 FROM data_batch');sources=q.fetchall()
        for source in sources:
            protected.add(config['hdfs']['root']+'/raw/source_sha256='+source['source_sha256'])
            if source['manifest_path']:protected.add(str(PurePosixPath(source['manifest_path']).parent))
        q.execute('SELECT experiment_id,sample_manifest FROM experiment');experiments=q.fetchall()
        for exp in experiments:
            if exp['sample_manifest'].startswith('/'):
                protected.add(str(PurePosixPath(exp['sample_manifest']).parent))
    for task in tasks:
        if task['status']!='FAILED' or not task['finished_at'] or task['finished_at']>now()-timedelta(days=7):continue
        final=PurePosixPath(task['output_path']);temporary=str(final.parent/('.'+task['run_id']+'.tmp'))
        if not temporary.startswith(config['hdfs']['root']+'/'):raise ManifestError('Temporary path escaped project HDFS root')
        if protected_path(temporary,protected):blocked.append(dict(path=temporary,reason='referenced or active'))
        else:candidates.append(dict(path=temporary,run_id=task['run_id'],reason='failed temporary older than seven days'))
    return dict(status='PASS',mode='DRY_RUN',deletion_authorized=False,current_release_id=current,
                retained_release_ids=[r['release_id'] for r in releases],protected_roots=sorted(protected),
                candidates=candidates,blocked=blocked,unique_source_reclaim_allowed=False,
                policy='Keep all READY/STAGING dependencies; recheck live tasks and references before any future deletion.',
                observed_at=stamp())
