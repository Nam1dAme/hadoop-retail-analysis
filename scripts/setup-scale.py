#!/usr/bin/env python3
"""Provision isolated benchmark control resources without altering production artifacts."""
import configparser
import getpass
import os
from pathlib import Path
import secrets
import sys
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT

with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as db,db.cursor() as q:
    q.execute('CREATE DATABASE IF NOT EXISTS behavior_scale CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
    for role,grants in [('writer','SELECT,INSERT,UPDATE,DELETE'),('migrator','ALL PRIVILEGES')]:
        path=ROOT/f'var/secrets/scale_{role}.cnf'
        if path.exists():cfg=configparser.ConfigParser();cfg.read(path);password=cfg['client']['password']
        else:
            password=secrets.token_hex(24)
            with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:f.write(f'[client]\nuser=behavior_scale_{role}\npassword={password}\n')
        q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',(f'behavior_scale_{role}','127.0.0.1',password))
        q.execute(f'GRANT {grants} ON behavior_scale.* TO %s@%s',(f'behavior_scale_{role}','127.0.0.1'))
print('Isolated scale database ready; credentials not printed')
