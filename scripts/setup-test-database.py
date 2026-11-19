#!/usr/bin/env python3
"""仅在本项目私有MariaDB实例创建三重隔离的集成测试资源。"""
from pathlib import Path
import getpass
import sys
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts._common import ROOT,db_endpoint,ensure_db_secret,load_dev_config

cfg=load_dev_config()
host,port,_=db_endpoint(cfg)
hdfs_uri=cfg['hdfs']['uri']
with pymysql.connect(unix_socket=str(ROOT/'var/mariadb/server.sock'),user=getpass.getuser(),autocommit=True) as db,db.cursor() as q:
    q.execute('CREATE DATABASE IF NOT EXISTS behavior_test_m123 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
    for role,grants in [('writer','SELECT, INSERT, UPDATE, DELETE'),('migrator','ALL PRIVILEGES')]:
        user=f'behavior_test_{role}'
        password=ensure_db_secret(ROOT/f'var/secrets/test_{role}.cnf',user,'behavior_test_m123',host=host,port=port)
        q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',(user,host,password));q.execute(f'GRANT {grants} ON behavior_test_m123.* TO %s@%s',(user,host))
config=ROOT/'var/fixtures/test.toml'
config.parent.mkdir(parents=True,exist_ok=True)
config.write_text(f'''baseline_version = "UBA-DOC-1.0.0"
synthetic = true
[database]
host = "{host}"
port = {port}
name = "behavior_test_m123"
[hdfs]
uri = "{hdfs_uri}"
root = "/behavior-test/m123"
[paths]
work_dir = "{ROOT}/var/tests/m123"
[source]
path = "{ROOT}/var/fixtures/sample-a.csv"
''')
print('测试数据库与配置已就绪；凭据未输出')
