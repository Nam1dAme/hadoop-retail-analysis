"""Contract tests against an isolated real MariaDB, not production release claims."""
from datetime import datetime,timezone
import json
import os
from uuid import uuid4
import pymysql
import pytest
from orchestration.common import ROOT,load
from orchestration.database import connect

pytestmark=pytest.mark.skipif(os.environ.get('BEHAVIOR_INTEGRATION')!='1',reason='Requires explicit isolated MariaDB integration execution')


def test_all_read_endpoints_and_bigints(api):
    client=api['client'];rid=api['release']
    paths=['/health','/releases','/releases/current','/overview','/trends','/entities','/entities/001/trend','/activity','/users/001/activity','/predictions','/experiments','/models/'+api['models']['LOGISTIC'],'/quality','/tasks','/reports/summary']
    for path in paths:
        response=client.get('/api/v1'+path,params={} if path in ('/health','/releases','/releases/current') else {'release_id':rid})
        assert response.status_code==200,(path,response.text)
        assert response.json()['meta']['request_id']==response.headers['X-Request-ID']
    data=client.get('/api/v1/overview',params={'release_id':rid}).json()['data']
    assert data['pv_count']=='2' and data['uv']=='2' and data['pv_buy_ratio']==.5
    user=client.get('/api/v1/users/001/activity',params={'release_id':rid}).json()['data'][0]
    assert user['first_ts']==user['last_ts']==datetime.fromtimestamp(1511539200,timezone.utc).isoformat().replace('+00:00','Z')
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


def test_exact_range_ranking_bound_ties_and_pagination(api):
    from datetime import date,timedelta
    import random
    from service.ranking import rank_entities
    randomizer=random.Random(42);rid=api['release'];base=date(2017,11,25)
    with connect(api['config'],ROOT/'var/secrets/test_writer.cnf') as db,db.cursor() as q:
        values=[]
        for day in range(4):
            for i in range(100):
                for kind in ('ITEM','CATEGORY'):
                    for behavior in ('pv','buy','fav'):
                        if behavior!='fav' and randomizer.random()<.3:continue
                        count=1 if behavior=='fav' else randomizer.randrange(1,30)
                        values.append((rid,base+timedelta(days=day),kind,str(1000+i),behavior,count))
        q.executemany('INSERT INTO entity_daily VALUES(%s,%s,%s,%s,%s,%s)',values)
    pool=api['pool']
    for start,end in ((base,base+timedelta(days=1)),(base,base+timedelta(days=4)),(base+timedelta(days=1),base+timedelta(days=3))):
        for kind in ('ITEM','CATEGORY'):
            for behavior in ('pv','buy','fav'):
                expected=pool.all('SELECT entity_id,SUM(event_count) AS event_count FROM entity_daily WHERE release_id=%s AND date>=%s AND date<%s AND entity_type=%s AND behavior_type=%s GROUP BY entity_id ORDER BY event_count DESC,entity_id ASC',(rid,start,end,kind,behavior))
                expected=[dict(entity_id=row['entity_id'],event_count=int(row['event_count'])) for row in expected]
                first=rank_entities(pool,rid,start,end,kind,behavior,11,chunk=7)
                assert first==expected[:11]
                position=[str(first[9]['event_count']),first[9]['entity_id'],10]
                second=rank_entities(pool,rid,start,end,kind,behavior,11,position,chunk=7)
                assert second==expected[10:21]
    # Equal totals must still order IDs correctly across all pages, even without early pruning.
    ordered=[];position=None
    while True:
        rows=rank_entities(pool,rid,base,base+timedelta(days=4),'ITEM','fav',17,position,chunk=7)
        if not rows:break
        ordered.extend(row['entity_id'] for row in rows)
        position=[str(rows[-1]['event_count']),rows[-1]['entity_id'],len(ordered)]
    assert ordered==[str(1000+i) for i in range(100)]
    with pytest.raises(TimeoutError):rank_entities(pool,rid,base,base+timedelta(days=4),'ITEM','fav',10,chunk=7,max_candidates=5)


def test_real_sql_timeout_has_actionable_error_without_claiming_db_down(api):
    class SlowPool(Pool):
        def all(self,sql,args=(),**kwargs):
            if sql.startswith('SELECT entity_id,event_count FROM entity_daily'):
                return super().all('SELECT SLEEP(0.1)',timeout=.01)
            return super().all(sql,args,**kwargs)
    pool=SlowPool(api['config'],ROOT/'var/secrets/test_reader.cnf')
    with TestClient(create_app(api['config'],pool=pool,cursor_key=b't'*32)) as client:
        response=client.get('/api/v1/entities',params={'release_id':api['release']})
        assert response.status_code==503
        assert response.json()['error']['code']=='DEPENDENCY_UNAVAILABLE'
        assert '查询超过时限' in response.json()['error']['message']
        assert response.json()['error']['request_id']==response.headers['X-Request-ID']
    assert api['pool'].one('SELECT 1 AS ok')['ok']==1
