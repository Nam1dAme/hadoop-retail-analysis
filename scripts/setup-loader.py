#!/usr/bin/env python3
"""Separate control writer, result loader, API reader and migration privileges."""
import configparser
import getpass
import os
from pathlib import Path
import secrets
import sys
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load
from orchestration.database import connect

CONTROL=('data_batch','task_run','resource_lease','quality_metric','experiment','model_run','model_epoch','model_metric','benchmark')
RESULTS=('event_cube','entity_daily','user_daily','range_summary','activity_daily','prediction','data_release','current_release','release_event','release_load')
secret=ROOT/'var/secrets/loader.cnf'
if secret.exists():cfg=configparser.ConfigParser();cfg.read(secret);password=cfg['client']['password']
else:
    password=secrets.token_hex(24)
    with os.fdopen(os.open(secret,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:f.write('[client]\nuser=behavior_loader\npassword='+password+'\n')
with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as db,db.cursor() as q:
    q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',('behavior_loader','127.0.0.1',password))
    q.execute('GRANT SELECT ON behavior_analytics.* TO %s@%s',('behavior_loader','127.0.0.1'))
    for table in RESULTS:q.execute('GRANT INSERT,UPDATE,DELETE ON behavior_analytics.'+table+' TO %s@%s',('behavior_loader','127.0.0.1'))
    # Grant table permissions before revoking database-wide DML, so active control writes keep working.
    for table in CONTROL:q.execute('GRANT INSERT,UPDATE,DELETE ON behavior_analytics.'+table+' TO %s@%s',('behavior_writer','127.0.0.1'))
    q.execute('SHOW GRANTS FOR %s@%s',('behavior_writer','127.0.0.1'));grants=[row[0] for row in q.fetchall()]
    if any(' ON `behavior_analytics`.* ' in line and 'INSERT' in line for line in grants):
        q.execute('REVOKE INSERT,UPDATE,DELETE ON behavior_analytics.* FROM %s@%s',('behavior_writer','127.0.0.1'))
config=load(ROOT/'config/development.toml')
with connect(config,secret) as db,db.cursor() as q:q.execute('SELECT 1')
print('Control writer / result loader / API reader / migrator privileges separated; secrets not printed')
