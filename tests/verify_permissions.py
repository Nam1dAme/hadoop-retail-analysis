"""Non-mutating permission probes against real production roles."""
from pathlib import Path
import sys
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,stamp
from orchestration.database import connect

config=load(ROOT/'config/development.toml');results={}
for role in ('writer','loader','reader'):
    with connect(config,ROOT/f'var/secrets/{role}.cnf') as db,db.cursor() as q:
        q.execute('SELECT COUNT(*) FROM data_release')
        allowed='resource_lease' if role=='writer' else 'prediction' if role=='loader' else None
        if allowed:q.execute('DELETE FROM '+allowed+' WHERE 1=0')
        denied=['prediction'] if role=='writer' else ['resource_lease'] if role=='loader' else ['prediction','resource_lease']
        for table in denied:
            try:q.execute('DELETE FROM '+table+' WHERE 1=0')
            except pymysql.MySQLError as error:assert error.args[0] in (1142,1143)
            else:raise AssertionError('Unexpected write privilege: '+role+' '+table)
        try:q.execute('CREATE TABLE _forbidden_permission_probe(id INT)')
        except pymysql.MySQLError as error:assert error.args[0] in (1142,1143)
        else:raise AssertionError('Unexpected DDL privilege')
        q.execute("SELECT LOAD_FILE('/etc/hostname') AS contents");assert q.fetchone()['contents'] is None
        results[role]='PASS'
atomic_json(ROOT/'var/production/permission-verification.json',dict(status='PASS',observed_at=stamp(),roles=results,mutated_rows=0))
print(results)
