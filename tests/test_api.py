"""Contract tests against an isolated real MariaDB, not production release claims."""
import configparser
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
import secrets
from uuid import uuid4
import pymysql
import pytest
from fastapi.testclient import TestClient
from orchestration.common import ROOT,load,now,encode,fingerprint
from orchestration.database import connect,migrate
from service.api import create_app
from service.database import Pool

pytestmark=pytest.mark.skipif(os.environ.get('BEHAVIOR_INTEGRATION')!='1',reason='Requires explicit isolated MariaDB integration execution')

@pytest.fixture(scope='module')
def api():
    config=load(ROOT/'var/fixtures/test.toml')
    with connect(config,ROOT/'var/secrets/test_migrator.cnf') as db:migrate(db)
    secret=ROOT/'var/secrets/test_reader.cnf'
    if not secret.exists():
        password=secrets.token_hex(24)
        with os.fdopen(os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:
            f.write(f'[client]\nuser=behavior_test_reader\npassword={password}\n')
    parsed=configparser.ConfigParser();parsed.read(secret)
    with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as admin,admin.cursor() as q:
        q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',('behavior_test_reader','127.0.0.1',parsed['client']['password']))
        q.execute('GRANT SELECT ON behavior_test_m123.* TO %s@%s',('behavior_test_reader','127.0.0.1'))
    experiment=str(uuid4());release=str(uuid4());staging=str(uuid4());retired=str(uuid4());models={k:str(uuid4()) for k in ('CONSTANT','LOGISTIC','GRU')}
    with connect(config,ROOT/'var/secrets/test_writer.cnf') as db,db.cursor() as q:
        q.execute('SELECT batch_id FROM data_batch LIMIT 1');batch=q.fetchone()['batch_id']
        selection={'reason':'synthetic API fixture','finalists':[{'algorithm':k,'model_run_id':v} for k,v in models.items()]}
        q.execute('INSERT INTO experiment VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                  (experiment,batch,'synthetic-api-fixture','synthetic',fingerprint(experiment),fingerprint({}),'SUCCEEDED',models['LOGISTIC'],now(),encode(selection),now(),now()))
        for k,v in models.items():
            q.execute('INSERT INTO model_run(model_run_id,experiment_id,algorithm,status,seed,config_hash,config_json,device,parameter_count,hardware_json,created_at,updated_at,threshold,training_seconds) VALUES(%s,%s,%s,%s,42,%s,%s,%s,%s,%s,%s,%s,.5,1)',
                      (v,experiment,k,'SUCCEEDED',fingerprint(v),'{"synthetic":true}','cpu',0 if k=='CONSTANT' else 15,'{}',now(),now()))
            for split in ('VALIDATION','TEST'):
                q.execute('INSERT INTO model_metric VALUES(%s,%s,%s,%s,.5,2,1,NULL,%s)',(v,split,'ALL','ap','0'*64))
        manifest={'samples':{k:{'rows':2,'positive_count':1} for k in ('TRAIN','VALIDATION','TEST')}}
        for rid,status in ((release,'READY'),(staging,'STAGING'),(retired,'RETIRED')):
            q.execute('INSERT INTO data_release VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                      (rid,batch,experiment,models['LOGISTIC'],status,'synthetic',fingerprint(rid),encode(manifest),'2017-12-03 16:00:00','2017-12-04 16:00:00',now(),now()))
        q.execute('UPDATE current_release SET release_id=%s,generation=generation+1 WHERE singleton_id=1',(release,))
        q.execute('INSERT INTO event_cube VALUES(%s,%s,0,%s,2)',(release,'2017-11-25','pv'))
        q.execute('INSERT INTO range_summary VALUES(%s,%s,%s,2,2,1,2)',(release,'2017-11-25','2017-12-04'))
        q.execute('INSERT INTO range_summary VALUES(%s,%s,%s,2,2,1,2)',(release,'2017-11-25','2017-11-26'))
        q.execute('INSERT INTO activity_daily VALUES(%s,%s,2,2,NULL)',(release,'2017-11-25'))
        q.execute('INSERT INTO user_daily VALUES(%s,%s,%s,1,0,0,0,1511539200,1511539200,1)',(release,'001','2017-11-25'))
        for entity in ('001','10','2'):
            q.execute('INSERT INTO entity_daily VALUES(%s,%s,%s,%s,%s,2)',(release,'2017-11-25','ITEM',entity,'pv'))
        for mode in ('TEST','FORECAST'):
            for i,user in enumerate(('001','10','2')):
                q.execute('INSERT INTO prediction VALUES(%s,%s,%s,%s,%s,%s,.5,1,%s,3,%s)',
                          (release,mode,models['LOGISTIC'],user,'2017-12-03 16:00:00','2017-12-04 16:00:00',None if mode=='FORECAST' else i%2,fingerprint([mode,user])))
    pool=Pool(config,secret)
    with TestClient(create_app(config,pool=pool,cursor_key=b'x'*32)) as client:
        yield dict(client=client,release=release,staging=staging,retired=retired,models=models,config=config,pool=pool)


def test_all_read_endpoints_and_bigints(api):
    client=api['client'];rid=api['release']
    paths=['/health','/releases','/releases/current','/overview','/trends','/entities','/entities/001/trend','/activity','/users/001/activity','/predictions','/experiments','/models/'+api['models']['LOGISTIC'],'/quality','/tasks','/reports/summary']
    for path in paths:
        response=client.get('/api/v1'+path,params={} if path in ('/health','/releases','/releases/current') else {'release_id':rid})
        assert response.status_code==200,(path,response.text)
        assert response.json()['meta']['request_id']==response.headers['X-Request-ID']
    data=client.get('/api/v1/overview',params={'release_id':rid}).json()['data']
    assert data['pv_count']=='2' and data['uv']=='2' and data['pv_buy_ratio']==.5
    rows=client.get('/api/v1/trends',params={'release_id':rid,'granularity':'hour'}).json()['data']
    assert len(rows)==9*24*4 and sum(int(r['event_count']) for r in rows)==2
    activity=client.get('/api/v1/activity',params={'release_id':rid}).json()['data']
    assert activity[0]['return_next_rate'] is None


def test_cursor_ties_release_validation_and_injection(api):
    client=api['client'];params={'release_id':api['release'],'limit':2}
    for route in ('entities','predictions'):
        first=client.get('/api/v1/'+route,params=params).json()
        key='entity_id' if route=='entities' else 'user_id'
        assert [r[key] for r in first['data']]==['001','10']
        second=client.get('/api/v1/'+route,params={**params,'cursor':first['meta']['next_cursor']}).json()
        assert [r[key] for r in second['data']]==['2'] and not second['meta']['has_more']
        assert client.get('/api/v1/'+route,params={**params,'limit':1,'cursor':first['meta']['next_cursor']}).status_code==422
    for extra in ({'unknown':'x'},{'start':'2017-12-04'},{'end':'2018-01-01'},{'release_id':"' OR 1=1"}):
        assert client.get('/api/v1/overview',params={'release_id':api['release'],**extra}).status_code==422
    for rid,status in ((api['staging'],409),(api['retired'],410),(str(uuid4()),404)):
        assert client.get('/api/v1/overview',params={'release_id':rid}).status_code==status
    assert client.get('/api/v1/entities/1%27/trend',params={'release_id':api['release']}).status_code==422
    assert client.post('/api/v1/predictions',json={}).status_code==405


def test_forecast_unknown_labels_model_scope_and_reader_permissions(api):
    client=api['client'];params={'release_id':api['release']}
    rows=client.get('/api/v1/predictions',params=params).json()['data']
    assert all(r['observed_label'] is None and r['label_status']=='UNOBSERVED' for r in rows)
    assert client.get('/api/v1/predictions',params={**params,'model_run_id':api['models']['GRU']}).status_code==422
    assert client.get('/api/v1/predictions',params={**params,'user_id':'999999'}).json()['data']==[]
    assert client.get('/api/v1/models/'+str(uuid4()),params=params).status_code==404
    with api['pool'].connection() as db,db.cursor() as q:
        with pytest.raises(pymysql.MySQLError):q.execute('INSERT INTO current_release VALUES(2,NULL,0)')
        with pytest.raises(pymysql.MySQLError):q.execute('CREATE TABLE forbidden_api_write(id INT)')


def test_pool_reconnect_restores_server_query_limit(api):
    pool=Pool(api['config'],ROOT/'var/secrets/test_reader.cnf',maximum=1)
    with pool.connection() as db:
        with db.cursor() as q:
            q.execute('SELECT CONNECTION_ID() AS id,@@max_statement_time AS seconds');before=q.fetchone()
        db.close()  # Close only this test's connection; the next checkout must reconnect.
    with pool.connection() as db:
        with db.cursor() as q:
            q.execute('SELECT CONNECTION_ID() AS id,@@max_statement_time AS seconds');after=q.fetchone()
    assert before['id']!=after['id']
    assert before['seconds']==after['seconds']==5
