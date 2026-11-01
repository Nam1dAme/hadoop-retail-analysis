import hashlib
import os
from pathlib import Path
import uuid
from datetime import datetime,timezone
from orchestration.common import *
from orchestration.pipeline import stage_run

def examine(path):
    p=Path(path).resolve(strict=True)
    if not p.is_file() or p.suffix.lower()!='.csv':raise ConfigError('Source must be a CSV regular file')
    before=p.stat();h=hashlib.sha256();rows=0;last=None
    with p.open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b);rows+=b.count(b'\n');last=b[-1]
    after=p.stat()
    if (before.st_size,before.st_mtime_ns,before.st_ino)!=(after.st_size,after.st_mtime_ns,after.st_ino):raise ManifestError('Source changed while hashing')
    if last is not None and last!=10:rows+=1
    return {'path':str(p),'sha256':h.hexdigest(),'bytes':after.st_size,'rows':rows,'mtime':datetime.fromtimestamp(after.st_mtime,timezone.utc).isoformat().replace('+00:00','Z'),'observed_at':stamp(),'downloaded_at':None,'provenance_pending':True}

def ingest(lease,source,source_url=None):
    info=examine(source);db=lease.db;lease.guard()
    with db.cursor() as q:
        q.execute('SELECT * FROM data_batch WHERE source_sha256=%s AND policy_version=%s',(info['sha256'],POLICY));batch=q.fetchone()
        if not batch:
            bid=str(uuid.uuid4());q.execute('INSERT INTO data_batch(batch_id,source_path,source_url,source_sha256,source_bytes,source_mtime,observed_at,policy_version,baseline_version,status,raw_rows,source_metadata,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(bid,info['path'],source_url,info['sha256'],info['bytes'],datetime.fromisoformat(info['mtime'].replace('Z','+00:00')).replace(tzinfo=None),now(),POLICY,BASELINE,'RUNNING',info['rows'],encode(info),now(),now()))
        else:bid=batch['batch_id']
    raw=f'{lease.config["hdfs"]["root"]}/raw/source_sha256={info["sha256"]}'
    raw_file={'path':raw+'/source.csv','sha256':info['sha256'],'bytes':info['bytes'],'rows':info['rows']}
    def archive(storage,temp,local,run_id):
        if not storage.exists(raw):
            parent=str(Path(raw).parent);storage.mkdir(parent);staged=parent+'/.source-'+run_id+'.tmp';storage.mkdir(staged)
            storage.put(info['path'],staged+'/source.csv')
            if storage.digest(staged+'/source.csv')!=(info['sha256'],info['bytes']):raise ManifestError('Archived raw source differs')
            meta=dict(info,source_url=source_url);storage.write_json(staged+'/source.json',meta,local/'source.json')
            storage.write_json(staged+'/manifest.json',{'baseline_version':BASELINE,'policy_version':POLICY,'schema_version':'1.0','source':raw_file,'source_metadata':meta,'status':'SUCCEEDED'},local/'raw-manifest.json')
            (local/'raw_SUCCESS').touch();storage.put(local/'raw_SUCCESS',staged+'/_SUCCESS');storage.rename(staged,raw)
        elif storage.digest(raw+'/source.csv')!=(info['sha256'],info['bytes']):raise ManifestError('Immutable archive changed')
        storage.write_json(temp+'/source.json',dict(info,source_url=source_url,raw_path=raw_file['path']),local/'stage-source.json')
        return {'source':raw_file,'counters':{'SOURCE':{'raw_rows':info['rows']}}}
    result=stage_run(lease,bid,'I00',[{k:info[k] for k in ('path','sha256','bytes','rows')}],{'source_url':source_url},archive)
    with db.cursor() as q:q.execute('UPDATE data_batch SET manifest_path=%s,updated_at=%s WHERE batch_id=%s',(str(Path(result['outputs'][0]['path']).parent/'manifest.json'),now(),bid))
    return {'status':'SUCCEEDED','batch_id':bid,'run_id':result['run_id'],'source':raw_file}
