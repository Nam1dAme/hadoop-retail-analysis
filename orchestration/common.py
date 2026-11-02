from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tomllib

ROOT=Path(__file__).resolve().parents[1]
BASELINE='UBA-DOC-1.0.0'
POLICY='behavior-v1'
class PipelineError(Exception):
    code='PIPELINE_FAILED';exit_code=5
class ConfigError(PipelineError):code='INVALID_CONFIG';exit_code=2
class DependencyError(PipelineError):code='DEPENDENCY_UNAVAILABLE';exit_code=3
class QualityError(PipelineError):code='QUALITY_GATE_FAILED';exit_code=4
class LockBusy(PipelineError):code='LOCK_BUSY';exit_code=6
class ManifestError(PipelineError):code='MANIFEST_INVALID';exit_code=5

def now():return datetime.now(timezone.utc).replace(tzinfo=None)
def stamp():return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
def encode(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def fingerprint(value):return hashlib.sha256(encode(value).encode()).hexdigest()
def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()
def atomic_json(path,value):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True);tmp=p.with_suffix(p.suffix+'.tmp');tmp.write_text(encode(value)+'\n');os.replace(tmp,p)
def commit():return subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
def load(path):
    c=tomllib.loads(Path(path).read_text())
    if c.get('baseline_version')!=BASELINE:raise ConfigError('Baseline mismatch')
    import re
    if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,63}',c['database']['name']):raise ConfigError('Invalid database name')
    root=c['hdfs']['root']
    if not root.startswith('/') or any(x in root for x in ('..',',','\n')):raise ConfigError('Unsafe HDFS namespace')
    work=Path(c.get('paths',{}).get('work_dir',ROOT/'var/production')).resolve()
    if not work.is_relative_to(ROOT/'var'):raise ConfigError('work_dir must be inside project var')
    c['paths']={'work_dir':str(work)}
    if (c.get('synthetic',False) or c.get('benchmark',False)) and (c['database']['name']=='behavior_analytics' or root=='/behavior/v1' or work==ROOT/'var/production'):
        raise ConfigError('Synthetic/benchmark work requires isolated DB, HDFS and work_dir')
    return c

def runtime_env():
    env=dict(os.environ)
    tools=ROOT/'var/tools'
    env.update(BEHAVIOR_ROOT=str(ROOT),JAVA_HOME=str(tools/'java11'),HADOOP_HOME=str(tools/'hadoop-3.4.2'),HADOOP_CONF_DIR=str(ROOT/'config/hadoop'))
    for name in ('HADOOP_COMMON_HOME','HADOOP_HDFS_HOME','HADOOP_MAPRED_HOME','HADOOP_YARN_HOME'):env[name]=env['HADOOP_HOME']
    env['PATH']=str(tools/'java11/bin')+':'+str(tools/'hadoop-3.4.2/bin')+':'+env['PATH']
    return env

def process_start(pid):
    try:return Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()[19]
    except (OSError,IndexError):return None

def implementation_fingerprint():
    paths=[p for folder in ('orchestration','ingest','ml') for p in (ROOT/folder).glob('*.py')]
    return fingerprint({str(p.relative_to(ROOT)):sha(p) for p in sorted(paths)})
