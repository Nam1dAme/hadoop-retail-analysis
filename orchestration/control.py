import os
import re
import threading
import time
import uuid
from pathlib import Path
from .common import now,process_start,LockBusy,PipelineError
from .database import connect

LOCK='behavior-heavy-task'
class Lease:
    def __init__(self,config,credentials):
        self.config=config;self.credentials=credentials;self.db=None;self.stop=threading.Event();self.error=None;self.task=None
    def __enter__(self):
        self.db=connect(self.config,self.credentials)
        with self.db.cursor() as q:
            q.execute('SELECT GET_LOCK(%s,0) AS acquired',(LOCK,))
            if q.fetchone()['acquired']!=1:self.db.close();raise LockBusy('Another heavy task owns resource lock')
            q.execute('SELECT CONNECTION_ID() AS id');self.connection_id=q.fetchone()['id']
            q.execute('UPDATE resource_lease SET generation=generation+1,owner_run_id=%s,heartbeat_at=%s,updated_at=%s WHERE singleton_id=1',(str(uuid.uuid4()),now(),now()))
            q.execute('SELECT generation FROM resource_lease WHERE singleton_id=1');self.generation=q.fetchone()['generation']
        self.worker=threading.Thread(target=self._heartbeat,daemon=True);self.worker.start();return self
    def _heartbeat(self):
        db=None;low_since=None
        try:
            db=connect(self.config,self.credentials)
            while not self.stop.wait(5):
                with db.cursor() as q:
                    q.execute('SELECT IS_USED_LOCK(%s) AS owner',(LOCK,))
                    if q.fetchone()['owner']!=self.connection_id:raise LockBusy('Lock ownership lost')
                    q.execute('UPDATE resource_lease SET heartbeat_at=%s,updated_at=%s WHERE singleton_id=1 AND generation=%s',(now(),now(),self.generation))
                    if q.rowcount!=1:raise LockBusy('Lease generation changed')
                    if self.task:q.execute('UPDATE task_run SET heartbeat_at=%s,updated_at=%s WHERE run_id=%s AND lease_generation=%s AND status=%s',(now(),now(),self.task,self.generation,'RUNNING'))
                mem=next(int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:'))
                if mem<1024**3:
                    low_since=low_since or time.monotonic()
                    if time.monotonic()-low_since>=30:raise PipelineError('Memory reserve below 1 GiB for 30s')
                else:low_since=None
                import shutil
                if shutil.disk_usage(self.config['paths']['work_dir']).free<10*1024**3:raise PipelineError('Local disk reserve below 10 GiB')
        except Exception as e:self.error=e
        finally:
            if db:db.close()
    def guard(self):
        if self.error:raise LockBusy('Lease heartbeat or resource guard failed') from self.error
        with self.db.cursor() as q:
            q.execute('SELECT IS_USED_LOCK(%s) AS owner',(LOCK,))
            if q.fetchone()['owner']!=self.connection_id:raise LockBusy('Lock owner changed')
            q.execute('SELECT generation FROM resource_lease WHERE singleton_id=1')
            if q.fetchone()['generation']!=self.generation:raise LockBusy('Lease fenced')
    def __exit__(self,*exc):
        self.stop.set();self.worker.join(timeout=35)
        try:
            with self.db.cursor() as q:
                q.execute('UPDATE resource_lease SET owner_run_id=NULL,updated_at=%s WHERE singleton_id=1 AND generation=%s',(now(),self.generation))
                q.execute('SELECT RELEASE_LOCK(%s)',(LOCK,))
        finally:self.db.close()

def alive(row):
    return row['pid'] is not None and process_start(row['pid'])==row['process_started_at']

def live_java(run_id):
    # HDFS客户端没有behavior.run.id属性，但暂存路径或冻结JAR路径包含run_id。
    token=run_id.encode()
    for p in Path('/proc').glob('[0-9]*/cmdline'):
        try:a=p.read_bytes().split(b'\0')
        except OSError:continue
        if a and Path(a[0].decode(errors='ignore')).name=='java' and any(token in x for x in a):return True
    return False
