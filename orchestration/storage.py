import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.request
import urllib.error
from .common import ROOT,runtime_env,PipelineError,ManifestError,encode

JAR=ROOT/'jobs/target/behavior-jobs.jar'
class Hdfs:
    def __init__(self,config,lease=None):self.config=config;self.lease=lease;self.env=runtime_env()
    def command(self,*args,check=True):
        if self.lease:self.lease.guard()
        p=subprocess.run(['hdfs','dfs','-fs',self.config['hdfs']['uri'],*map(str,args)],env=self.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        if check and p.returncode:raise PipelineError('HDFS command failed: '+p.stderr.decode(errors='replace')[-1200:])
        return p
    def exists(self,p):return self.command('-test','-e',p,check=False).returncode==0
    def json(self,p):return json.loads(self.command('-cat',p).stdout)
    def mkdir(self,p):self.command('-mkdir','-p',p)
    def put(self,local,remote):self.command('-put',local,remote)
    def rename(self,src,dst):
        if self.exists(dst):raise ManifestError('Refuse overwrite of immutable HDFS output')
        self.command('-mv',src,dst)
    def write_json(self,p,value,local):
        local=Path(local);local.parent.mkdir(parents=True,exist_ok=True);local.write_text(encode(value)+'\n');self.put(local,p)
    def digest(self,p):
        h=hashlib.sha256();n=0
        with subprocess.Popen(['hdfs','dfs','-cat',p],env=self.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as process:
            while b:=process.stdout.read(4*1024*1024):
                if self.lease:self.lease.guard()
                h.update(b);n+=len(b)
            if process.wait()!=0:raise PipelineError('Cannot verify archived source')
        return h.hexdigest(),n
    def inspect(self,path):
        p=subprocess.run(['hadoop','jar',str(JAR),'cn.behavior.Files','-Dfs.defaultFS='+self.config['hdfs']['uri'],'inspect',path],env=self.env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        if p.returncode:raise ManifestError('Cannot inspect HDFS output: '+p.stderr.decode(errors='replace')[-1000:])
        rows=[json.loads(line) for line in p.stdout.decode().splitlines()]
        if len(rows)>10000:raise ManifestError('Output exceeds 10000 files')
        return sorted(rows,key=lambda x:x['path'])
    def dump(self,files):
        paths=[f['path'] if isinstance(f,dict) else f for f in files]
        if not paths:return
        log=Path(self.config['paths']['work_dir'])/'dump-errors.log';log.parent.mkdir(parents=True,exist_ok=True)
        with log.open('ab') as err:
            p=subprocess.Popen(['hadoop','jar',str(JAR),'cn.behavior.Files','-Dfs.defaultFS='+self.config['hdfs']['uri'],'dump',paths[0]],env=self.env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True)
            try:
                p.stdin.write('\n'.join(paths)+'\n');p.stdin.close()
                for i,line in enumerate(p.stdout):
                    if self.lease and i%1024==0:self.lease.guard()
                    yield line.rstrip('\n')
                if p.wait()!=0:raise PipelineError('SequenceFile dump failed, see dump-errors.log')
            finally:
                p.stdout.close()
                if p.poll() is None:p.terminate();p.wait(timeout=10)

def application(config,app_id):
    base=config.get('yarn',{}).get('api','http://127.0.0.1:18088')
    try:
        with urllib.request.urlopen(base+'/ws/v1/cluster/apps/'+app_id,timeout=5) as r:return json.load(r)['app']
    except urllib.error.HTTPError as e:
        if e.code==404:return None
        raise

def discover(config,name):
    base=config.get('yarn',{}).get('api','http://127.0.0.1:18088')
    with urllib.request.urlopen(base+'/ws/v1/cluster/apps?applicationTags=behavior',timeout=5) as r:apps=json.load(r).get('apps') or {}
    return [x for x in apps.get('app',[]) if x['name']==name]

def run_job(stage,inputs,output,params,lease,log,update_app):
    args=['hadoop','jar',params.get('jar_path',str(JAR)),'cn.behavior.Jobs',f'-Dbehavior.run.id={params["run_id"]}',f'-Dbehavior.reducers={params["reducers"]}']
    if params.get('source_sha256'):args.append('-Dbehavior.source.sha256='+params['source_sha256'])
    if stage=='J01' and params.get('split_bytes'):args.append('-Dbehavior.split.bytes='+str(params['split_bytes']))
    args+=['-Dfs.defaultFS='+lease.config['hdfs']['uri'],'-Dmapreduce.job.tags=behavior',stage,','.join(x['path'] for x in inputs),output]
    log=Path(log);log.parent.mkdir(parents=True,exist_ok=True);app_id=None
    with log.open('w') as out, log.open() as watch:
        p=subprocess.Popen(args,env=runtime_env(),stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
        buffer=''
        try:
            while True:
                lease.guard();buffer=(buffer+watch.read())[-65536:]
                ids=re.findall(r'application_\d+_\d+',buffer)
                if ids and ids[-1]!=app_id:app_id=ids[-1];update_app(app_id)
                if p.poll() is not None:break
                time.sleep(.5)
            if p.returncode:raise PipelineError(f'{stage} failed; inspect {log}')
        except BaseException:
            if p.poll() is None:
                os.killpg(p.pid,signal.SIGTERM)
                try:p.wait(timeout=10)
                except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
            if app_id:
                subprocess.run(['yarn','application','-kill',app_id],env=runtime_env(),stdout=out,stderr=out,timeout=30)
            raise
    return app_id
