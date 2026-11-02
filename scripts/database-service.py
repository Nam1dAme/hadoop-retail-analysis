#!/usr/bin/env python3
"""复用系统MariaDB程序，管理本项目独立实例，不接触系统实例。"""
import argparse
import configparser
import getpass
import os
from pathlib import Path
import secrets
import subprocess
import time
import pymysql

ROOT=Path(__file__).resolve().parents[1]
BASE=ROOT/'var/mariadb'
SOCKET=BASE/'server.sock'
SECRET=ROOT/'var/secrets'

def connect():
    return pymysql.connect(unix_socket=str(SOCKET),user=getpass.getuser(),autocommit=True)

def running():
    try:
        with connect() as c:c.ping()
        return True
    except pymysql.MySQLError:return False

def start():
    if running():return
    BASE.mkdir(parents=True,exist_ok=True)
    if not (BASE/'data/mysql').is_dir():raise SystemExit('请先执行 init')
    with (BASE/'launcher.log').open('ab') as log:
        subprocess.Popen(['/usr/sbin/mariadbd','--no-defaults',f'--datadir={BASE}/data',
            f'--socket={SOCKET}',f'--pid-file={BASE}/server.pid',f'--log-error={BASE}/server.log',
            '--bind-address=127.0.0.1','--port=3307','--innodb-buffer-pool-size=256M',
            '--max-connections=30','--character-set-server=utf8mb4','--collation-server=utf8mb4_bin',
            '--skip-name-resolve'],stdout=log,stderr=log,start_new_session=True)
    for _ in range(100):
        if running():return
        time.sleep(.2)
    raise SystemExit('启动失败，请检查var/mariadb/server.log')

def init():
    BASE.mkdir(parents=True,exist_ok=True)
    data=BASE/'data'
    if not (data/'mysql').is_dir():
        if data.exists() and any(data.iterdir()):raise SystemExit('拒绝初始化非空数据目录')
        subprocess.run(['/usr/bin/mariadb-install-db','--no-defaults',f'--datadir={data}',
            '--auth-root-authentication-method=socket',f'--auth-root-socket-user={getpass.getuser()}',
            '--skip-test-db'],check=True,stdout=subprocess.DEVNULL)
    start()
    SECRET.mkdir(parents=True,exist_ok=True,mode=0o700)
    with connect() as c, c.cursor() as q:
        q.execute('CREATE DATABASE IF NOT EXISTS behavior_analytics CHARACTER SET utf8mb4 COLLATE utf8mb4_bin')
        for role,grants in [('reader','SELECT'),('writer','SELECT, INSERT, UPDATE, DELETE'),('migrator','ALL PRIVILEGES')]:
            path=SECRET/f'{role}.cnf'
            if path.exists():
                cfg=configparser.ConfigParser();cfg.read(path);password=cfg['client']['password']
            else:
                password=secrets.token_hex(24)
                text=f'[client]\nuser=behavior_{role}\npassword={password}\nhost=127.0.0.1\nport=3307\ndatabase=behavior_analytics\n'
                fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                with os.fdopen(fd,'w') as f:f.write(text)
            user=f'behavior_{role}'
            q.execute('CREATE USER IF NOT EXISTS %s@%s IDENTIFIED BY %s',(user,'127.0.0.1',password))
            effective='SELECT' if role=='writer' and (SECRET/'loader.cnf').exists() else grants
            q.execute(f'GRANT {effective} ON behavior_analytics.* TO %s@%s',(user,'127.0.0.1'))
    print('独立数据库及三个项目账号已就绪；凭据仅写入var/secrets')

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['init','start','stop','status']);a=p.parse_args()
    if a.action=='init':init()
    elif a.action=='start':start()
    elif a.action=='stop':
        if running():
            with connect() as c, c.cursor() as q:q.execute('SHUTDOWN')
    else:
        with connect() as c,c.cursor() as q:
            q.execute('SELECT VERSION(), @@port, @@innodb_buffer_pool_size, @@max_connections')
            print(q.fetchone())

if __name__=='__main__':main()
