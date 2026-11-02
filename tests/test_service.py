import os
import json
from datetime import date
import pytest
from service.security import Cursors,CursorError
from service.api import WindowParams,public

def test_signed_cursor_binds_release_filters_and_position():
    signer=Cursors(b'x'*32);token=signer.encode('release-and-filter',[.5,'001'])
    assert signer.decode(token,'release-and-filter')==[.5,'001']
    with pytest.raises(CursorError):signer.decode(token,'different-release')
    with pytest.raises(CursorError):signer.decode(token[:-2]+'xx','release-and-filter')
    with pytest.raises(CursorError):signer.decode('not a cursor','release-and-filter')

def test_api_precision_window_and_null_semantics():
    assert public({'event_count':2**60,'score':.125,'return_next_rate':None,'user_id':'001'})=={'event_count':str(2**60),'score':.125,'return_next_rate':None,'user_id':'001'}
    with pytest.raises(ValueError):WindowParams(release_id='00000000-0000-0000-0000-000000000001',start=date(2017,12,4))
    with pytest.raises(ValueError):WindowParams(release_id='00000000-0000-0000-0000-000000000001',invented='field')
