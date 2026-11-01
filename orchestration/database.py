import configparser
from pathlib import Path
import pymysql
from .common import ROOT,sha,now,ConfigError

def connect(config,credentials):
    cfg=configparser.ConfigParser();cfg.read(credentials)
    if 'client' not in cfg:raise ConfigError('Missing database credential file')
    p=cfg['client'];d=config['database']
    return pymysql.connect(host=d['host'],port=d['port'],user=p['user'],password=p['password'],database=d['name'],charset='utf8mb4',autocommit=True,cursorclass=pymysql.cursors.DictCursor,connect_timeout=5,read_timeout=30,write_timeout=30,init_command="SET time_zone='+00:00'")

def migrate(connection):
    with connection.cursor() as q:
        q.execute('CREATE TABLE IF NOT EXISTS schema_migration (version VARCHAR(64) PRIMARY KEY,sha256 CHAR(64) NOT NULL,applied_at DATETIME(6) NOT NULL)')
        for p in sorted((ROOT/'migrations').glob('*.sql')):
            digest=sha(p);q.execute('SELECT sha256 FROM schema_migration WHERE version=%s',(p.name,));row=q.fetchone()
            if row:
                if row['sha256']!=digest:raise ConfigError('Applied migration checksum changed')
                continue
            # DDL隐式提交；中断的部分迁移不可默默重跑，交由人工审计恢复。
            for sql in p.read_text().split(';'):
                if sql.strip():q.execute(sql)
            q.execute('INSERT INTO schema_migration VALUES(%s,%s,%s)',(p.name,digest,now()))
    return {'status':'SUCCEEDED','migrations':len(list((ROOT/'migrations').glob('*.sql')))}

def require_current(connection):
    with connection.cursor() as q:
        q.execute('SELECT version,sha256 FROM schema_migration');actual={r['version']:r['sha256'] for r in q.fetchall()}
    for p in (ROOT/'migrations').glob('*.sql'):
        if actual.get(p.name)!=sha(p):raise ConfigError('Database migrations required')

def backup(config,credentials,path):
    """mysqldump不接受client.database；临时凭据0600且不出现在进程参数。"""
    import os
    import subprocess
    import tempfile
    cfg=configparser.ConfigParser();cfg.read(credentials)
    selected=configparser.ConfigParser();selected['client']={k:v for k,v in cfg['client'].items() if k in ('user','password')}
    selected['client']['host']=config['database']['host'];selected['client']['port']=str(config['database']['port'])
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w+',dir=path.parent,suffix='.cnf') as f:
        selected.write(f);f.flush()
        with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as output:
            subprocess.run(['mariadb-dump','--defaults-extra-file='+f.name,'--single-transaction','--skip-lock-tables',config['database']['name']],stdout=output,check=True)
