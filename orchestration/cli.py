import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from .common import *
from .database import connect,migrate,require_current,backup as database_backup
from .control import Lease
from .pipeline import pipeline,STAGES
from .samples import prepare
from ingest.source import ingest,examine

def parser():
    p=argparse.ArgumentParser(prog='behaviorctl')
    p.add_argument('--config',default=str(ROOT/'config/development.toml'));p.add_argument('--baseline',required=True)
    p.add_argument('--credentials',default=str(ROOT/'var/secrets/writer.cnf'));p.add_argument('--dry-run',action='store_true')
    sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('migrate')
    i=sub.add_parser('ingest');i.add_argument('--source');i.add_argument('--source-url')
    for name in ('profile','pipeline','resume'):
        q=sub.add_parser(name);q.add_argument('--batch-id',required=True);q.add_argument('--reducers',type=int,default=2);q.add_argument('--through',choices=STAGES,default='J04summary' if name!='profile' else 'J02');q.add_argument('--audit',action='store_true');q.add_argument('--split-bytes',type=int)
    for name in ('prepare-samples','status'):
        q=sub.add_parser(name);q.add_argument('--batch-id',required=True)
    return p

def execute(args):
    c=load(args.config)
    if args.baseline!=BASELINE:raise ConfigError('Unsupported baseline')
    if hasattr(args,'batch_id'):
        import uuid
        try:uuid.UUID(args.batch_id)
        except ValueError:raise ConfigError('Invalid batch UUID') from None
    if args.dry_run:
        if args.command=='ingest':source=examine(args.source or c['source']['path'])
        else:source=None
        return {'status':'DRY_RUN','command':args.command,'source':source,'baseline_version':BASELINE,'plan':list(STAGES) if args.command in ('pipeline','resume') else [args.command]}
    work=Path(c['paths']['work_dir']);work.mkdir(parents=True,exist_ok=True)
    if args.command=='migrate':
        backup=work/'backups'/('pre-migration-'+stamp().replace(':','')+'.sql');backup.parent.mkdir(parents=True,exist_ok=True)
        database_backup(c,args.credentials,backup)
        with connect(c,args.credentials) as db:return migrate(db)
    with connect(c,args.credentials) as db:
        require_current(db)
        if args.command=='status':
            with db.cursor() as q:
                q.execute('SELECT batch_id,status,raw_rows,accepted_rows,rejected_rows,in_scope_rows,outside_rows,manifest_path FROM data_batch WHERE batch_id=%s',(args.batch_id,));batch=q.fetchone()
                q.execute('SELECT run_id,stage,attempt,status,application_id,error_code FROM task_run WHERE batch_id=%s ORDER BY created_at',(args.batch_id,));tasks=q.fetchall()
            return {'status':'SUCCEEDED','batch':batch,'tasks':tasks}
    dirty=subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)
    if dirty.strip() and not c.get('synthetic'):raise ConfigError('Commit changes before formal execution')
    with Lease(c,args.credentials) as lease:
        if args.command=='ingest':return ingest(lease,args.source or c['source']['path'],args.source_url)
        if args.command=='prepare-samples':return prepare(lease,args.batch_id)
        if args.command=='resume':
            with lease.db.cursor() as q:q.execute('SELECT source_path,source_url FROM data_batch WHERE batch_id=%s',(args.batch_id,));row=q.fetchone()
            if not row:raise ConfigError('Unknown batch')
            info=ingest(lease,row['source_path'],row['source_url'])
            if info['batch_id']!=args.batch_id:raise ManifestError('Original source now differs')
        return pipeline(lease,args.batch_id,args.reducers,'J02' if args.command=='profile' else args.through,args.audit,args.split_bytes)

def main():
    args=parser().parse_args()
    try:result=execute(args)
    except PipelineError as e:
        print(encode({'status':'FAILED','error_code':e.code,'message':str(e)}));return e.exit_code
    except (OSError,subprocess.CalledProcessError) as e:
        print(encode({'status':'FAILED','error_code':'DEPENDENCY_UNAVAILABLE','message':type(e).__name__}));return 3
    except Exception as e:
        print(encode({'status':'FAILED','error_code':'UNEXPECTED_ERROR','message':type(e).__name__}));return 5
    print(encode(result));return 0
if __name__=='__main__':sys.exit(main())
