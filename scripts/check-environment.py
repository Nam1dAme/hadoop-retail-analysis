#!/usr/bin/env python3
"""验证本项目实际依赖、CUDA与数据库最小权限，不运行生产业务。"""
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import tempfile
from datetime import datetime,timezone
import pymysql
import torch

ROOT=Path(__file__).resolve().parents[1]
assert platform.python_version_tuple()[:2]==('3','12')
java=subprocess.check_output([str(ROOT/'var/tools/java11/bin/java'),'-version'],stderr=subprocess.STDOUT,text=True).splitlines()[0]
assert '11.0.' in java
env=dict(os.environ,JAVA_HOME=str(ROOT/'var/tools/java11'),HADOOP_CONF_DIR=str(ROOT/'config/hadoop'),BEHAVIOR_ROOT=str(ROOT))
hadoop=subprocess.check_output([str(ROOT/'var/tools/hadoop-3.4.2/bin/hadoop'),'version'],env=env,text=True).splitlines()[0]
assert hadoop=='Hadoop 3.4.2'
from fastapi import FastAPI
from fastapi.testclient import TestClient
app=FastAPI()
@app.get('/probe')
def probe():return {'ok':True}
with TestClient(app) as client:assert client.get('/probe').json()=={'ok':True}
import streamlit,plotly,numpy
report={'java':java,'hadoop':hadoop,'web_dependencies':'PASS','baseline':'UBA-DOC-1.0.0','observed_at':datetime.now(timezone.utc).isoformat(),
        'python':platform.python_version(),'packages':{d.metadata['Name']:d.version for d in importlib.metadata.distributions()}}
assert torch.cuda.is_available(),'CUDA不可访问，请在普通WSL或获准的沙箱外执行'
assert torch.cuda.device_count()==1
props=torch.cuda.get_device_properties(0)
assert '4060 Laptop' in props.name
report['gpu']={'name':props.name,'total_memory_mib':props.total_memory/1024**2,'runtime':torch.version.cuda}
torch.set_num_threads(4);torch.manual_seed(42)
class Probe(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.cat=torch.nn.Embedding(16385,16,padding_idx=0)
        self.behavior=torch.nn.Embedding(5,8,padding_idx=0)
        self.hour=torch.nn.Embedding(25,4,padding_idx=0)
        self.proj=torch.nn.Linear(29,64)
        self.gru=torch.nn.GRU(64,64,batch_first=True)
        self.head=torch.nn.Sequential(torch.nn.Linear(78,32),torch.nn.ReLU(),torch.nn.Dropout(.1),torch.nn.Linear(32,1))
    def forward(self,c,b,h,g,n):
        x=torch.cat([self.cat(c),self.behavior(b),self.hour(h),g],dim=-1)
        x,_=self.gru(torch.relu(self.proj(x)))
        return self.head(torch.cat([x[:,-1],n],dim=-1)).squeeze(-1)
m=Probe().cuda();opt=torch.optim.AdamW(m.parameters(),lr=.001)
x=(torch.randint(1,16385,(32,128),device='cuda'),torch.randint(1,5,(32,128),device='cuda'),
   torch.randint(1,25,(32,128),device='cuda'),torch.rand(32,128,1,device='cuda'),torch.randn(32,14,device='cuda'))
y=torch.arange(32,device='cuda')%2
losses=[]
for amp in [False,True]:
    old=m.head[-1].weight.detach().clone()
    scaler=torch.amp.GradScaler('cuda',enabled=amp)
    opt.zero_grad()
    with torch.autocast('cuda',dtype=torch.float16,enabled=amp):z=m(*x)
    loss=torch.nn.functional.binary_cross_entropy_with_logits(z.float(),y.float())
    assert torch.isfinite(loss)
    scaler.scale(loss).backward();scaler.unscale_(opt)
    torch.nn.utils.clip_grad_norm_(m.parameters(),1)
    scaler.step(opt);scaler.update();torch.cuda.synchronize()
    assert not torch.equal(old,m.head[-1].weight)
    losses.append(loss.item())
m.eval()
with torch.no_grad():expected=m(*x).cpu()
with tempfile.TemporaryDirectory(dir=ROOT/'var') as tmp:
    path=Path(tmp)/'state.pt'
    torch.save({k:v.cpu() for k,v in m.state_dict().items()},path)
    copy=Probe().cuda();copy.load_state_dict(torch.load(path,map_location='cpu',weights_only=True));copy.eval()
    with torch.no_grad():assert torch.allclose(expected,copy(*x).cpu(),atol=1e-6,rtol=0)
report['gpu'].update(fp32_amp_checkpoint='PASS',losses=losses,parameters=sum(p.numel() for p in m.parameters()),
    allocated_mib=torch.cuda.max_memory_allocated()/1024**2,reserved_mib=torch.cuda.max_memory_reserved()/1024**2)

def db(role):return pymysql.connect(read_default_file=str(ROOT/f'var/secrets/{role}.cnf'),autocommit=True)
hardened=(ROOT/'var/secrets/loader.cnf').exists()
with db('migrator') as c,c.cursor() as q:
    q.execute('CREATE TABLE _environment_probe (id INT PRIMARY KEY)')
try:
    with db('writer') as c,c.cursor() as q:
        if hardened:q.execute('UPDATE resource_lease SET heartbeat_at=heartbeat_at WHERE singleton_id=0')
        else:q.execute('INSERT INTO _environment_probe VALUES (1)')
    with db('reader') as c,c.cursor() as q:
        q.execute('SELECT * FROM _environment_probe');assert q.fetchall()==(() if hardened else ((1,),))
        try:q.execute('INSERT INTO _environment_probe VALUES (2)')
        except pymysql.MySQLError as e:assert e.args[0] in (1142,1143)
        else:raise AssertionError('reader unexpectedly writable')
    with db('writer') as c,c.cursor() as q:
        try:q.execute('CREATE TABLE _environment_forbidden (id INT)')
        except pymysql.MySQLError as e:assert e.args[0] in (1142,1143)
        else:raise AssertionError('writer unexpectedly has DDL permission')
    with db('migrator') as c,c.cursor() as q:
        q.execute('SELECT VERSION(),@@port,@@innodb_buffer_pool_size,@@max_connections')
        version,port,pool,connections=q.fetchone()
        report['database']=dict(version=version,port=port,buffer_pool_bytes=pool,max_connections=connections,role_isolation='PASS')
finally:
    with db('migrator') as c,c.cursor() as q:q.execute('DROP TABLE _environment_probe')
report['rss_peak_mib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024
report['status']='PASS'
report['code_commit']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
path=ROOT/'var/environment-probe.json';path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k!='packages'},ensure_ascii=False,indent=2))
