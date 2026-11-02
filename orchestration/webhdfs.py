"""Streaming local WebHDFS transport for backup/restore, avoiding a JVM per file."""
import hashlib
import http.client
import urllib.error
import urllib.parse
import urllib.request
from .common import ManifestError


class LocalDataNodeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,request,fp,code,message,headers,newurl):
        location=urllib.parse.urlsplit(newurl)
        # This deployment binds the single DataNode to loopback but advertises its host name.
        if location.scheme=='http' and location.port==19864:
            newurl=urllib.parse.urlunsplit(('http','127.0.0.1:19864',location.path,location.query,location.fragment))
        return super().redirect_request(request,fp,code,message,headers,newurl)


class WebHdfs:
    def __init__(self,base='http://127.0.0.1:19870',user='hadoop'):
        self.base=base.rstrip('/');self.user=user

    def url(self,path,operation,**params):
        return self.base+'/webhdfs/v1'+urllib.parse.quote(path,safe='/')+'?'+urllib.parse.urlencode(dict(op=operation,**{'user.name':self.user},**params))

    def open(self,path):
        return urllib.request.build_opener(LocalDataNodeRedirect()).open(self.url(path,'OPEN'),timeout=30)

    def put(self,path,source,size,guard=None):
        request=urllib.request.Request(self.url(path,'CREATE',overwrite='false',createparent='true'),data=b'',method='PUT')
        try:
            with urllib.request.urlopen(request,timeout=30):
                raise ManifestError('Expected WebHDFS create redirect')
        except urllib.error.HTTPError as error:
            if error.code!=307:raise
            location=urllib.parse.urlsplit(error.headers['Location'])
        if location.scheme!='http' or location.port!=19864:raise ManifestError('Unexpected DataNode upload target')
        connection=http.client.HTTPConnection('127.0.0.1',location.port,timeout=30)
        digest=hashlib.sha256();count=0
        try:
            connection.putrequest('PUT',location.path+'?'+location.query)
            connection.putheader('Content-Length',str(size));connection.putheader('Content-Type','application/octet-stream');connection.endheaders()
            while block:=source.read(1024**2):
                if guard:guard()
                connection.send(block);digest.update(block);count+=len(block)
            result=connection.getresponse();result.read()
            if result.status!=201 or count!=size:raise ManifestError('Incomplete WebHDFS restoration')
        finally:connection.close()
        return digest.hexdigest()
