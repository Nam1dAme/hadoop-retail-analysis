import json
import os
import threading
from types import SimpleNamespace
from uuid import uuid4
import pytest
from orchestration.common import ROOT,now,fingerprint,PipelineError,ManifestError
from orchestration.database import connect
from publication.release import load_rows,publish_pointer
from test_api import api

pytestmark=pytest.mark.skipif(os.environ.get('BEHAVIOR_INTEGRATION')!='1',reason='Requires explicit isolated MariaDB release integration execution')

def test_staging_loader_interruption_resume_and_idempotence(api,monkeypatch):
    rid=api['staging'];config=api['config']
    def rows():
        for i in range(1501):yield(rid,str(10000+i),'2017-11-25',1,0,0,0,1511539200,1511539200,1)
    with connect(config,ROOT/'var/secrets/test_writer.cnf') as db:
        lease=SimpleNamespace(db=db,guard=lambda:None)
        monkeypatch.setenv('BEHAVIOR_TEST_STOP_LOAD','user_daily')
        with pytest.raises(PipelineError):load_rows(lease,rid,'user_daily','user_daily','a'*64,rows())
        with db.cursor() as q:
            q.execute('SELECT rows_loaded,complete FROM release_load WHERE release_id=%s AND dataset=%s',(rid,'user_daily'))
            assert q.fetchone()==dict(rows_loaded=1000,complete=0)
        assert api['client'].get('/api/v1/overview',params={'release_id':rid}).status_code==409
        monkeypatch.delenv('BEHAVIOR_TEST_STOP_LOAD')
        assert load_rows(lease,rid,'user_daily','user_daily','a'*64,rows())==1501
        assert load_rows(lease,rid,'user_daily','user_daily','a'*64,iter(()))==1501
        with pytest.raises(ManifestError):load_rows(lease,rid,'user_daily','user_daily','b'*64,rows())
        with db.cursor() as q:
            q.execute('SELECT COUNT(*) AS n FROM user_daily WHERE release_id=%s',(rid,));assert q.fetchone()['n']==1501


def test_atomic_publish_cas_and_rollback(api):
    config=api['config'];targets=[str(uuid4()),str(uuid4())]
    datasets=('event_cube','entity_daily','user_daily','range_summary','activity_daily','TEST-CONSTANT','TEST-LOGISTIC','TEST-GRU','FORECAST')
    with connect(config,ROOT/'var/secrets/test_writer.cnf') as db,db.cursor() as q:
        q.execute('SELECT * FROM data_release WHERE release_id=%s',(api['release'],));original=q.fetchone()
        for rid in targets:
            q.execute('INSERT INTO data_release VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)',
                      (rid,original['batch_id'],original['experiment_id'],original['selected_model_id'],'STAGING','synthetic',fingerprint(rid),original['manifest_json'],original['forecast_as_of'],original['forecast_window_end'],now()))
            for dataset in datasets:q.execute('INSERT INTO release_load VALUES(%s,%s,%s,0,1,%s)',(rid,dataset,'0'*64,now()))
        q.execute('SELECT generation FROM current_release WHERE singleton_id=1');generation=q.fetchone()['generation']
    barrier=threading.Barrier(2);outcomes=[]
    def worker(rid):
        with connect(config,ROOT/'var/secrets/test_writer.cnf') as db:
            barrier.wait()
            try:publish_pointer(db,rid,generation);outcomes.append(('published',rid))
            except ManifestError:outcomes.append(('conflict',rid))
    threads=[threading.Thread(target=worker,args=(rid,)) for rid in targets]
    for thread in threads:thread.start()
    for thread in threads:thread.join(timeout=20);assert not thread.is_alive()
    assert sorted(x[0] for x in outcomes)==['conflict','published']
    with connect(config,ROOT/'var/secrets/test_writer.cnf') as db,db.cursor() as q:
        q.execute('SELECT * FROM current_release WHERE singleton_id=1');current=q.fetchone()
        assert current['generation']==generation+1
        assert current['release_id']==next(rid for status,rid in outcomes if status=='published')
        losing=next(rid for status,rid in outcomes if status=='conflict')
        q.execute('SELECT status FROM data_release WHERE release_id=%s',(losing,));assert q.fetchone()['status']=='STAGING'
        publish_pointer(db,api['release'],generation+1,action='ROLLBACK')
        q.execute('SELECT release_id,generation FROM current_release WHERE singleton_id=1');assert q.fetchone()==dict(release_id=api['release'],generation=generation+2)
        with pytest.raises(ManifestError):publish_pointer(db,api['retired'],generation+2,action='ROLLBACK')
        with pytest.raises(ManifestError):publish_pointer(db,api['staging'],generation+2)
