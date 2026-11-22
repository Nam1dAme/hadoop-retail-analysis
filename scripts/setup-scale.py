#!/usr/bin/env python3
"""Provision isolated benchmark control resources without altering production artifacts."""
import getpass
from pathlib import Path
import sys
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts._common import ROOT,db_endpoint,ensure_db_secret,load_dev_config

host,port,_=db_endpoint(load_dev_config())
with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as db,db.cursor() as q:
    q.execute('CREATE DATABASE IF NOT EXISTS behavior_scale CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
    for role,grants in [('writer','SELECT,INSERT,UPDATE,DELETE'),('migrator','ALL PRIVILEGES')]:
        user=f'behavior_scale_{role}'
        password=ensure_db_secret(ROOT/f'var/secrets/scale_{role}.cnf',user,'behavior_scale',host=host,port=port)
        q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',(user,host,password))
        q.execute(f'GRANT {grants} ON behavior_scale.* TO %s@%s',(user,host))
print('Isolated scale database ready; credentials not printed')
