"""Read-only audit of actual benchmark counters, skew, resource coverage and fresh jobs."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
from orchestration.common import ROOT,load,sha,fingerprint,atomic_json,stamp,commit,ManifestError
from orchestration.webhdfs import WebHdfs

STAGES={'J01','J02','J03','J03rank','J04users','J04summary'}
COUNTERS=('MAP_INPUT_RECORDS','MAP_OUTPUT_RECORDS','REDUCE_INPUT_RECORDS','REDUCE_SHUFFLE_BYTES','SPILLED_RECORDS',
          'CPU_MILLISECONDS','GC_TIME_MILLIS','MAP_PHYSICAL_MEMORY_BYTES_MAX','REDUCE_PHYSICAL_MEMORY_BYTES_MAX')

def audit_tier(tier,guard=lambda:None):
    directory=ROOT/'var/scale'/tier;path=directory/'verification.json';report=json.loads(path.read_text())
    expected=json.loads((ROOT/'var/production/m123-verification.json').read_text())['source']['rows'] if tier=='full' else int(tier)
    if report['status']!='PASS' or report['rows']!=expected or report['sampled']!=(tier!='full'):raise ManifestError('Scale execution identity differs')
    if report['resource_observations']<1 or report['peak_increment_bytes'] is None or report['min_memory_available'] is None:raise ManifestError('Missing scale resource observations')
    config=load(directory/'config.toml')
    if not config.get('benchmark') or config['hdfs']['root']!='/behavior-scale/v1/'+tier:raise ManifestError('Scale namespace is not isolated')
    start=datetime.fromisoformat(report['started_at']);end=datetime.fromisoformat(report['finished_at']);web=WebHdfs();jobs=[]
    histories={r['application_id']:r for r in report['task_history']}
    for task in report['tasks']:
        if not task['application_id']:continue
        guard()
        local=directory/'runs'/task['run_id']/'manifest.json';raw=local.read_bytes();manifest=json.loads(raw)
        remote=manifest['output_path']
        if not remote.startswith(config['hdfs']['root']+'/') or task['status']!='SUCCEEDED':raise ManifestError('Invalid scale task output')
        with web.open(remote+'/manifest.json') as stream:archived=stream.read(2*1024**2+1)
        if archived!=raw:raise ManifestError('Local and archived scale manifests differ')
        with web.open(remote+'/_SUCCESS') as stream:
            if stream.read(1)!=b'':raise ManifestError('Invalid success marker')
        if manifest['application_id']!=task['application_id'] or manifest['run_id']!=task['run_id'] or manifest['batch_id']!=report['batch_id']:raise ManifestError('Scale application binding differs')
        if manifest['status']!='SUCCEEDED' or manifest['stage']!=task['stage'] or manifest['sampled']!=(tier!='full'):raise ManifestError('Scale manifest state differs')
        if fingerprint(manifest['config'])!=manifest['config_sha256'] or fingerprint(manifest['inputs'])!=manifest['input_hash']:raise ManifestError('Scale manifest fingerprints differ')
        if not start<=datetime.fromisoformat(manifest['created_at'])<=end:raise ManifestError('Reused stage cannot count as fresh scale throughput')
        counters=manifest.get('counters',{}).get('org.apache.hadoop.mapreduce.TaskCounter',{})
        if 'REDUCE_SHUFFLE_BYTES' not in counters:raise ManifestError('Shuffle evidence missing')
        metrics={key:counters.get(key) for key in COUNTERS};history=histories[task['application_id']]
        inputs=[r['input_records'] for r in history['reducers']]
        if not inputs or any(x is None or x<0 for x in inputs):raise ManifestError('Reducer input evidence missing')
        if sum(inputs)!=counters['REDUCE_INPUT_RECORDS']:raise ManifestError('Reducer inputs do not match job counter')
        total=sum(inputs);least=min(inputs);largest=max(inputs)
        skew=dict(inputs=inputs,max_fraction=largest/total if total else None,max_to_min=largest/least if least else None,
                  ratio_reason='zero denominator' if not least else None,task_durations=history['task_durations'])
        jobs.append(dict(stage=task['stage'],run_id=task['run_id'],application_id=task['application_id'],manifest_sha256=hashlib.sha256(raw).hexdigest(),
                         counters=metrics,unreported_counters=[key for key,value in metrics.items() if value is None],skew=skew))
    if {job['stage'] for job in jobs}!=STAGES or len(jobs)!=6:raise ManifestError('Six physical analysis jobs are required per tier')
    result=dict(status='PASS',tier=tier,rows=expected,execution_report_sha256=sha(path),fresh_jobs=True,jobs=jobs,
                seconds=report['seconds'],resource_observations=report['resource_observations'],min_memory_available=report['min_memory_available'],
                peak_increment_bytes=report['peak_increment_bytes'],disk_admission=report['disk_admission'],verified_at=stamp())
    atomic_json(directory/'audit.json',result);return result

def audit_scale(guard=lambda:None):
    output=ROOT/'var/scale/audit.json';index=ROOT/'var/scale/verification.json'
    report=dict(status='RUNNING',baseline_version='UBA-DOC-1.0.0',code_commit=commit(),started_at=stamp(),tiers=[])
    atomic_json(output,report)
    try:
        execution=json.loads(index.read_text())
        if execution['status']!='PASS' or {r['tier'] for r in execution['tiers']}!={'1000000','10000000','full'}:raise ManifestError('Complete three-tier execution is required')
        for tier in ('1000000','10000000','full'):report['tiers'].append(audit_tier(tier,guard))
        report.update(status='PASS',execution_report_sha256=sha(index),verified_at=stamp(),scope='Archived job metadata, Shuffle/spill/CPU/GC, reducer skew, fresh-run timing and sampled resource coverage')
        atomic_json(output,report);return report
    except BaseException as error:
        report.update(status='FAIL',error_type=type(error).__name__,message=str(error),verified_at=stamp());atomic_json(output,report);raise
