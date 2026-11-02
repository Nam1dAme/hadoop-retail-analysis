import json
import urllib.error
import urllib.parse
import urllib.request


class ApiFault(Exception):
    def __init__(self,code,message,request_id=None):
        self.code,self.message,self.request_id=code,message,request_id


class BehaviorAPI:
    def __init__(self,base,release=None):
        self.base=base.rstrip('/');self.release=release

    def get(self,path,**params):
        if self.release and path not in ('/health','/releases','/releases/current'):
            params['release_id']=self.release['release_id']
        params={k:v for k,v in params.items() if v is not None and v!=''}
        url=self.base+path+('?' + urllib.parse.urlencode(params) if params else '')
        try:
            with urllib.request.urlopen(url,timeout=7) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            try:
                detail=json.load(error)['error']
                raise ApiFault(detail['code'],detail['message'],detail.get('request_id')) from None
            except (ValueError,KeyError):
                raise ApiFault('HTTP_ERROR','查询服务返回了无法读取的响应。') from None
        except (urllib.error.URLError,TimeoutError):
            raise ApiFault('DEPENDENCY_UNAVAILABLE','无法连接查询服务，请确认服务已启动。') from None
