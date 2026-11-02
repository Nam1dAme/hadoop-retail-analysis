"""Restore local backup to private MariaDB:3308 and an isolated HDFS root."""
from datetime import datetime
import getpass
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tarfile
import time
import uuid
import pymysql
from fastapi.testclient import TestClient
from orchestration.common import ROOT,atomic_json,sha,stamp,ManifestError
from orchestration.storage import Hdfs
from orchestration.webhdfs import WebHdfs
from service.api import create_app
from service.database import Pool


def restore(lease,backup_manifest=None):
    report=json.loads(Path(backup_manifest or ROOT/'var/production/backup.json').read_text())
    archive=Path(report['archive']).resolve(strict=True)
    if not archive.is_relative_to(ROOT/'var/backups') or sha(archive)!=report['sha256']:
        raise ManifestError('Untrusted or changed backup archive')
    identity=str(uuid.uuid4());directory=ROOT/'var/recovery'/identity;directory.mkdir(parents=True,exist_ok=False)
    remote='/behavior-recovery/v1/'+identity;socket=directory/'server.sock';data=directory/'data'
    expected={x['path']:x for x in report['hdfs_files']};seen=set();storage=Hdfs(lease.config,lease);web=WebHdfs()
    counts=report['table_counts']
    if any(not table.replace('_','').isalnum() for table in counts):raise ManifestError('Unexpected backup table identifier')
    started=time.monotonic();sql=directory/'database.sql';hdfs_bytes=0;server=None
    try:
        with tarfile.open(archive,mode='r|gz') as tar:
            for member in tar:
                lease.guard()
                if not member.isfile():raise ManifestError('Only regular backup members are allowed')
                stream=tar.extractfile(member)
                if member.name=='database.sql':
                    digest=hashlib.sha256()
                    with sql.open('xb') as out:
                        while block:=stream.read(1024**2):lease.guard();out.write(block);digest.update(block)
                    if digest.hexdigest()!=report['database_sha256']:raise ManifestError('Restored SQL hash differs')
                elif member.name.startswith('hdfs/'):
                    original='/'+member.name[len('hdfs/'):]
                    if '..' in Path(original).parts or not original.startswith(lease.config['hdfs']['root']+'/'):
                        raise ManifestError('Restoration dependency escaped project namespace')
                    item=expected.get(original)
                    if not item or original in seen or item['bytes']!=member.size:raise ManifestError('Unexpected backup artifact')
                    restored=remote+original
                    if web.put(restored,stream,member.size,lease.guard)!=item['sha256']:raise ManifestError('Restored HDFS hash differs')
                    seen.add(original);hdfs_bytes+=member.size
                elif member.name.startswith('project/'):
                    # Code/config evidence is checked by the archive checksum, not executed.
                    while stream.read(1024**2):lease.guard()
                else:raise ManifestError('Unknown backup member')
        if seen!=set(expected):raise ManifestError('Missing HDFS backup dependencies')
        subprocess.run(['/usr/bin/mariadb-install-db','--no-defaults','--datadir='+str(data),
                        '--auth-root-authentication-method=socket','--auth-root-socket-user='+getpass.getuser(),'--skip-test-db'],stdout=subprocess.DEVNULL,check=True)
        with (directory/'server.log').open('ab') as log:
            server=subprocess.Popen(['/usr/sbin/mariadbd','--no-defaults','--datadir='+str(data),'--socket='+str(socket),
                     '--pid-file='+str(directory/'server.pid'),'--bind-address=127.0.0.1','--port=3308',
                     '--innodb-buffer-pool-size=128M','--max-connections=10','--skip-name-resolve',
                     '--character-set-server=utf8mb4','--collation-server=utf8mb4_bin'],stdout=log,stderr=log,start_new_session=True)
        admin=None
        for _ in range(100):
            lease.guard()
            try:admin=pymysql.connect(unix_socket=str(socket),user=getpass.getuser(),autocommit=True);break
            except pymysql.MySQLError:
                if server.poll() is not None:raise RuntimeError('Private recovery database exited')
                time.sleep(.2)
        if admin is None:raise RuntimeError('Private recovery database did not start')
        with admin,admin.cursor() as q:q.execute('CREATE DATABASE behavior_recovery CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        with sql.open('rb') as inp,(directory/'import.log').open('wb') as errors:
            child=subprocess.Popen(['/usr/bin/mariadb','--no-defaults','--socket='+str(socket),'--user='+getpass.getuser(),'behavior_recovery'],stdin=inp,stdout=subprocess.DEVNULL,stderr=errors)
            while child.poll() is None:lease.guard();time.sleep(.5)
            if child.returncode:raise RuntimeError('Backup SQL import failed; inspect recovery import log')
        actual={};password=secrets.token_hex(24)
        with pymysql.connect(unix_socket=str(socket),user=getpass.getuser(),database='behavior_recovery',autocommit=True,cursorclass=pymysql.cursors.DictCursor) as restored,restored.cursor() as q:
            for table,count in counts.items():
                q.execute('SELECT COUNT(*) AS n FROM `'+table+'`');actual[table]=q.fetchone()['n']
                if actual[table]!=count:raise ManifestError('Restored table count differs: '+table)
            q.execute('UPDATE resource_lease SET owner_run_id=NULL,heartbeat_at=NULL')
            q.execute('CREATE USER %s@%s IDENTIFIED BY %s',('recovery_reader','127.0.0.1',password))
            q.execute('GRANT SELECT ON behavior_recovery.* TO %s@%s',('recovery_reader','127.0.0.1'))
        secret=directory/'reader.cnf'
        with os.fdopen(os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:f.write('[client]\nuser=recovery_reader\npassword='+password+'\n')
        config=dict(lease.config,database=dict(host='127.0.0.1',port=3308,name='behavior_recovery'))
        reader=Pool(config,secret)
        original=create_app(lease.config,cursor_key=b'o'*32);copy=create_app(config,pool=reader,cursor_key=b'r'*32)
        comparisons=[]
        with TestClient(original) as left,TestClient(copy) as right:
            for route in ('/releases/current','/overview','/predictions','/experiments','/reports/summary'):
                params={} if route=='/releases/current' else {'release_id':report['release_id']}
                a=left.get('/api/v1'+route,params=params);b=right.get('/api/v1'+route,params=params)
                if a.status_code!=200 or b.status_code!=200 or a.json()['data']!=b.json()['data']:
                    raise ManifestError('Recovered API result differs: '+route)
                comparisons.append(route)
        duration=time.monotonic()-started
        if duration>4*3600:raise ManifestError('Local restore exceeded four-hour RTO target')
        result=dict(status='PASS',scope='local-copy restore only',independent_backup_verified=False,
                    release_id=report['release_id'],archive_sha256=report['sha256'],database_port=3308,
                    restored_hdfs_root=remote,verified_hdfs_files=len(seen),verified_hdfs_bytes=hdfs_bytes,
                    table_counts=actual,api_comparisons=comparisons,rto_seconds=duration,verified_at=stamp())
        atomic_json(ROOT/'var/production/recovery.json',result)
        return result
    finally:
        if server is not None and server.poll() is None:
            try:
                with pymysql.connect(unix_socket=str(socket),user=getpass.getuser(),autocommit=True) as admin,admin.cursor() as q:q.execute('SHUTDOWN')
            finally:server.wait(timeout=30)
        if (ROOT/'var/production/recovery.json').exists():
            result=json.loads((ROOT/'var/production/recovery.json').read_text())
            if result.get('restored_hdfs_root')==remote and result.get('status')=='PASS':
                # Only the uniquely owned, fully verified disposable restoration is removed.
                storage.command('-rm','-r','-skipTrash',remote)
                shutil.rmtree(directory)
