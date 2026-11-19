#!/usr/bin/env python3
"""Separate control writer, result loader, API reader and migration privileges."""
import getpass
from pathlib import Path
import sys
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load
from orchestration.database import connect
from scripts._common import db_endpoint,ensure_db_secret,load_dev_config

CONTROL=('data_batch','task_run','resource_lease','quality_metric','experiment','model_run','model_epoch','model_metric','benchmark')
RESULTS=('event_cube','entity_daily','user_daily','range_summary','activity_daily','prediction','data_release','current_release','release_event','release_load')
secret=ROOT/'var/secrets/loader.cnf'
host,port,db_name=db_endpoint(load_dev_config())
password=ensure_db_secret(secret,'behavior_loader',db_name,host=host,port=port)
with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as db,db.cursor() as q:
    q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',('behavior_loader',host,password))
    q.execute(f'GRANT SELECT ON {db_name}.* TO %s@%s',('behavior_loader',host))
    for table in RESULTS:q.execute(f'GRANT INSERT,UPDATE,DELETE ON {db_name}.'+table+' TO %s@%s',('behavior_loader',host))
    # Grant table permissions before revoking database-wide DML, so active control writes keep working.
    for table in CONTROL:q.execute(f'GRANT INSERT,UPDATE,DELETE ON {db_name}.'+table+' TO %s@%s',('behavior_writer',host))
    q.execute('SHOW GRANTS FOR %s@%s',('behavior_writer',host));grants=[row[0] for row in q.fetchall()]
    if any(f' ON `{db_name}`.* ' in line and 'INSERT' in line for line in grants):
        q.execute(f'REVOKE INSERT,UPDATE,DELETE ON {db_name}.* FROM %s@%s',('behavior_writer',host))
config=load(ROOT/'config/development.toml')
with connect(config,secret) as db,db.cursor() as q:q.execute('SELECT 1')
print('Control writer / result loader / API reader / migrator privileges separated; secrets not printed')
