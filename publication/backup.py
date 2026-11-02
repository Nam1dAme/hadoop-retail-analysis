"""Local, verified compressed backup. It never claims independent-disk protection."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time
import uuid
from orchestration.common import ROOT,atomic_json,sha,encode,stamp,ManifestError
from orchestration.database import backup as sql_backup
from orchestration.storage import Hdfs
from orchestration.webhdfs import WebHdfs
from .validation import prepare_offline_checks


class DigestReader:
    def __init__(self,source,guard):
        self.source=source;self.digest=hashlib.sha256();self.count=0;self.guard=guard
    def read(self,n=-1):
        self.guard();block=self.source.read(n);self.digest.update(block);self.count+=len(block);return block


def make_backup(lease):
    started=time.monotonic()
    atomic_json(ROOT/'var/production/backup.json',dict(status='RUNNING',started_at=stamp(),independent_backup_verified=False))
    prepare_offline_checks(lease.db)
    if (ROOT/'var/scale/verification.json').exists():
        from .scale_audit import audit_scale
        audit_scale(lease.guard)
    release=json.loads((ROOT/'var/production/release.json').read_text())
    with lease.db.cursor() as q:
        q.execute("SELECT * FROM data_release WHERE release_id=%s AND status='READY'",(release['release_id'],));row=q.fetchone()
        if not row:raise ManifestError('Backup requires a published READY release')
        q.execute("SELECT status FROM task_run WHERE status IN ('RUNNING','PENDING')");assert not q.fetchall()
        q.execute("SELECT output_path FROM task_run WHERE batch_id=%s AND status='SUCCEEDED'",(row['batch_id'],));roots=[x['output_path'] for x in q.fetchall()]
        q.execute('SHOW TABLES');tables=[next(iter(x.values())) for x in q.fetchall()]
        table_counts={}
        for table in tables:
            if not table.replace('_','').isalnum():raise ManifestError('Unexpected table identifier')
            q.execute('SELECT COUNT(*) AS n FROM `'+table+'`');table_counts[table]=q.fetchone()['n']
    directory=ROOT/'var/backups'/('release-'+row['release_id']+'-'+stamp().replace(':',''))
    directory.mkdir(parents=True,exist_ok=False)
    models=json.loads((ROOT/'var/production/models.json').read_text());roots.append(models['archive'])
    raw=json.loads((ROOT/'var/production/ingest.json').read_text())['source']
    storage=Hdfs(lease.config,lease);web=WebHdfs();file_map={}
    def add(path,size,digest):
        if not path.startswith(lease.config['hdfs']['root']+'/') or '..' in Path(path).parts:raise ManifestError('Backup dependency escaped project namespace')
        if path in file_map and file_map[path]!=(size,digest):raise ManifestError('Conflicting backup dependency')
        file_map[path]=(size,digest)
    for root in sorted(set(roots)):
        raw_manifest=storage.command('-cat',root+'/manifest.json').stdout
        manifest=json.loads(raw_manifest)
        add(root+'/manifest.json',len(raw_manifest),hashlib.sha256(raw_manifest).hexdigest())
        for item in manifest['outputs']:
            path=item['path'] if item['path'].startswith('/') else root+'/'+item['path']
            add(path,item['bytes'],item['sha256'])
    add(raw['path'],raw['bytes'],raw['sha256'])
    for name in ('manifest.json','source.json','_SUCCESS'):
        path=str(Path(raw['path']).parent/name);content=storage.command('-cat',path).stdout
        add(path,len(content),hashlib.sha256(content).hexdigest())
    release_manifest=storage.command('-cat',row['manifest_path']).stdout
    add(row['manifest_path'],len(release_manifest),hashlib.sha256(release_manifest).hexdigest())
    # DB dump includes immutable result tables and the corresponding control snapshot.
    # Use the existing migrator role only for this explicit offline administrative operation.
    dump=directory/'database.sql'
    sql_backup(lease.config,ROOT/'var/secrets/migrator.cnf',dump)
    dump_hash=sha(dump);archive=directory/'backup.tar.gz'
    with os.fdopen(os.open(archive,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as output,gzip.GzipFile(fileobj=output,mode='wb',compresslevel=1,mtime=0) as compressed,tarfile.open(fileobj=compressed,mode='w|',copybufsize=1024**2) as tar:
        tar.add(dump,arcname='database.sql',recursive=False)
        local_files=[ROOT/'README.md',ROOT/'uv.lock',*sorted((ROOT/'config').rglob('*')),*sorted((ROOT/'var/history-rewrite').rglob('commit-map.json')),*sorted((ROOT/'var/history-rewrite').rglob('*.bundle'))]
        local_files += [ROOT/'var/production'/name for name in ('m123-verification.json','models.json','model-verification.json','forecast.json','release.json','release-verification.json','cuda-recovery-test.json','api-verification.json','entity-ranking-verification.json','api-load-test.json','permission-verification.json','cgroup-memory-observation.json')]
        for name in ('verification.json','audit.json'):
            if (ROOT/'var/scale'/name).exists():local_files.append(ROOT/'var/scale'/name)
        for folder in ('browser','browser-selected-model','browser-user-detail'):
            local_files.extend(sorted((ROOT/'var/production'/folder).glob('*.json')))
            local_files.extend(sorted((ROOT/'var/production'/folder).glob('*.png')))
        if (ROOT/'var/fixtures/hotspot-verification.json').exists():local_files.append(ROOT/'var/fixtures/hotspot-verification.json')
        if (ROOT/'var/production/acceptance/status.json').exists():local_files.append(ROOT/'var/production/acceptance/status.json')
        for name in ('ranking-development-regression.log','entity-lookup-contract.log','user-time-contract.log','api-timeout-contract.log','java-npy-contracts.log'):
            if (ROOT/'var/production'/name).exists():local_files.append(ROOT/'var/production'/name)
        bundle=directory/'source.bundle';subprocess.run(['git','bundle','create',str(bundle),'--all'],cwd=ROOT,check=True);local_files.append(bundle)
        for local in local_files:
            if local.is_file():tar.add(local,arcname='project/'+str(local.relative_to(ROOT)),recursive=False)
        for index,(path,(size,expected)) in enumerate(sorted(file_map.items())):
            info=tarfile.TarInfo('hdfs'+path);info.size=size;info.mode=0o600
            with web.open(path) as stream:
                reader=DigestReader(stream,lease.guard);tar.addfile(info,reader)
                if reader.count!=size or reader.digest.hexdigest()!=expected:raise ManifestError('Backup source changed: '+path)
            if index%100==0:print(encode(dict(backup_files=index,total=len(file_map))),flush=True)
    # Verify the archive itself before removing the reconstructible uncompressed SQL copy.
    archive_hash=sha(archive)
    report=dict(status='PASS',release_id=row['release_id'],batch_id=row['batch_id'],archive=str(archive),sha256=archive_hash,
                archive_bytes=archive.stat().st_size,database_sha256=dump_hash,hdfs_files=[dict(path=p,bytes=v[0],sha256=v[1]) for p,v in sorted(file_map.items())],
                table_counts=table_counts,
                local_copy_only=True,independent_backup_verified=False,reason='User confirmed no separate physical disk',
                seconds=time.monotonic()-started,created_at=stamp())
    atomic_json(directory/'manifest.json',report)
    dump.unlink();bundle.unlink()
    atomic_json(ROOT/'var/production/backup.json',report)
    return report
