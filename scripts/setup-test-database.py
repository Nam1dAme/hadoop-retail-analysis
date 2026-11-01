#!/usr/bin/env python3
"""仅在本项目私有MariaDB实例创建三重隔离的集成测试资源。"""
from pathlib import Path
import configparser
import getpass
import os
import secrets
import pymysql
ROOT=Path(__file__).resolve().parents[1]
with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as db,db.cursor() as q:
    q.execute('CREATE DATABASE IF NOT EXISTS behavior_test_m123 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
    for role,grants in [('writer','SELECT, INSERT, UPDATE, DELETE'),('migrator','ALL PRIVILEGES')]:
        path=ROOT/f'var/secrets/test_{role}.cnf'
        if path.exists():
            cfg=configparser.ConfigParser();cfg.read(path);password=cfg['client']['password']
        else:
            password=secrets.token_hex(24)
            with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:f.write(f'[client]\nuser=behavior_test_{role}\npassword={password}\nhost=127.0.0.1\nport=3307\ndatabase=behavior_test_m123\n')
        user=f'behavior_test_{role}';q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',(user,'127.0.0.1',password));q.execute(f'GRANT {grants} ON behavior_test_m123.* TO %s@%s',(user,'127.0.0.1'))
config=ROOT/'var/fixtures/test.toml'
config.write_text(f'''baseline_version = "UBA-DOC-1.0.0"
synthetic = true
[database]
host = "127.0.0.1"
port = 3307
name = "behavior_test_m123"
[hdfs]
uri = "hdfs://127.0.0.1:19000"
root = "/behavior-test/m123"
[paths]
work_dir = "{ROOT}/var/tests/m123"
[source]
path = "{ROOT}/var/fixtures/sample-a.csv"
''')
print('测试数据库与配置已就绪；凭据未输出')
