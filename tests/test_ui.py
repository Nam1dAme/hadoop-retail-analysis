import os
import pytest
from streamlit.testing.v1 import AppTest
from orchestration.common import ROOT
from app.client import BehaviorAPI,ApiFault
from test_api import api

pytestmark=pytest.mark.skipif(os.environ.get('BEHAVIOR_INTEGRATION')!='1',reason='Requires explicit isolated API/UI integration execution')

def test_seven_pages_use_real_release_api(api,monkeypatch):
    calls=[]
    def bridge(self,path,**params):
        if self.release and path not in ('/health','/releases','/releases/current'):params['release_id']=self.release['release_id']
        params={k:v for k,v in params.items() if v is not None and v!=''}
        calls.append((path,params))
        result=api['client'].get('/api/v1'+path,params=params)
        if result.status_code!=200:
            value=result.json()['error'];raise ApiFault(value['code'],value['message'],value['request_id'])
        return result.json()
    monkeypatch.setattr(BehaviorAPI,'get',bridge)
    app=AppTest.from_file(str(ROOT/'app/main.py'),default_timeout=30)
    app.run();assert not app.exception
    for page in ('overview','entities','activity','predictions','experiments','quality','versions'):
        app.radio(key='page').set_value(page).run()
        assert not app.exception,[e.message for e in app.exception]
        assert not app.error,[e.value for e in app.error]
    assert any(path=='/trends' for path,_ in calls)
    assert all(p.get('release_id')==api['release'] for path,p in calls if path not in ('/health','/releases','/releases/current'))

def test_no_release_empty_state_has_no_fake_values(monkeypatch):
    def missing(*args,**kwargs):raise ApiFault('NO_ACTIVE_RELEASE','尚无可用数据版本。')
    monkeypatch.setattr(BehaviorAPI,'get',missing)
    app=AppTest.from_file(str(ROOT/'app/main.py'),default_timeout=20).run()
    assert not app.exception
    assert len(app.radio(key='page').options)==7
    html='\n'.join(x.proto.body for x in app.get('html'))
    assert '当前不展示演示数据' in html
