#!/usr/bin/env python3
"""Actual published-data API checks, including entity and user detail cross-checks."""
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
import re
import sys
import time
import urllib.request
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,atomic_json,stamp,commit,sha,fingerprint

started=time.monotonic();producer=commit();release=json.loads((ROOT/'var/production/release.json').read_text());rid=release['release_id']
pipeline=json.loads((ROOT/'var/production/pipeline.json').read_text());source=json.loads((ROOT/'var/production/ingest.json').read_text())['source'];records=[]
def get(route,params=None,scoped=True):
    params=dict(params or {})
    if scoped:params['release_id']=rid
    tick=time.monotonic()
    with urllib.request.urlopen('http://127.0.0.1:8010/api/v1'+route+('?' + urllib.parse.urlencode(params) if params else ''),timeout=7) as response:
        raw=response.read();assert len(raw)<=2*1024**2;value=json.loads(raw)
        assert value['meta']['request_id']==response.headers['X-Request-ID']
    if scoped:assert value['meta']['release_id']==rid
    records.append(dict(route=re.sub(r'/[0-9]+(?=/|$)','/{id}',route),parameter_hash=fingerprint(params),seconds=time.monotonic()-tick,body_sha256=__import__('hashlib').sha256(raw).hexdigest()))
    return value

get('/health',scoped=False);assert get('/releases/current',scoped=False)['data']['release_id']==rid
assert any(r['release_id']==rid for r in get('/releases',scoped=False)['data'])
overview=get('/overview')['data'];assert int(overview['uv'])==pipeline['analysis']['users']
assert sum(int(overview[k+'_count']) for k in ('pv','buy','cart','fav'))==pipeline['analysis']['scoped_rows']
assert len(get('/trends',{'granularity':'day'})['data'])==36
entities=get('/entities',{'entity_type':'ITEM','behavior':'pv','limit':2})['data'];assert entities
trend=get('/entities/'+entities[0]['entity_id']+'/trend',{'entity_type':'ITEM'})['data']
assert sum(int(r['pv_count']) for r in trend)==int(entities[0]['event_count'])
activity=get('/activity')['data'];assert len(activity)==9 and activity[-1]['return_next_users'] is None
assert sum(int(r['first_observed_users']) for r in activity)==pipeline['analysis']['users']
forecast=get('/predictions',{'mode':'FORECAST','limit':2})['data'];assert forecast
for row in forecast:assert row['observed_label'] is None and row['label_status']=='UNOBSERVED' and row['as_of'].endswith('Z')
user=forecast[0];history=get('/users/'+user['user_id']+'/activity',{'start':'2017-12-02','end':'2017-12-04'})['data']
assert sum(int(row[k+'_count']) for row in history for k in ('pv','buy','cart','fav'))==int(user['history_event_count'])
for row in history:
    first=datetime.fromisoformat(row['first_ts']);last=datetime.fromisoformat(row['last_ts'])
    assert row['first_ts'].endswith('Z') and row['last_ts'].endswith('Z') and first<=last
    assert str(first.astimezone(timezone(timedelta(hours=8))).date())==row['date']
experiments=get('/experiments')['data'];assert len(experiments['models'])==3
for model in experiments['models']:
    assert get('/models/'+model['model_run_id'])['data']['model_run_id']==model['model_run_id']
    for row in get('/predictions',{'mode':'TEST','model_run_id':model['model_run_id'],'limit':2})['data']:
        assert row['observed_label'] in (0,1) and row['label_status']=='OBSERVED'
quality=get('/quality')['data'];assert int(quality['raw_rows'])==source['rows'] and int(quality['in_scope_rows'])==pipeline['analysis']['scoped_rows']
tasks=get('/tasks',{'limit':100})['data'];expected={r['application_id'] for r in pipeline['stages'].values() if r['application_id']}
assert {r['application_id'] for r in tasks if r['application_id']}==expected and len(expected)==8
summary=get('/reports/summary')['data'];assert summary['overview']==overview and summary['release']['release_id']==rid
result=dict(status='PASS',baseline_version='UBA-DOC-1.0.0',release_id=rid,code_commit=producer,environment_sha256=sha(ROOT/'config/environment.lock.json'),
            records=records,seconds=time.monotonic()-started,verified_at=stamp(),scope='All 15 production GET routes; published counts, entity totals, user 48h history and UTC timestamp cross-checks')
atomic_json(ROOT/'var/production/api-verification.json',result);print(json.dumps({k:v for k,v in result.items() if k!='records'}))
