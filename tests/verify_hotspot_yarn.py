"""Actual synthetic YARN salting equivalence; never used as real population evidence."""
import json
import subprocess
import sys
from pathlib import Path
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,runtime_env
from orchestration.control import Lease
from orchestration.storage import Hdfs,application

config=load(ROOT/'var/fixtures/test.toml');identity=str(uuid.uuid4());local=ROOT/'var/fixtures'/('hotspot-'+identity+'.csv')
with local.open('w') as f:
    for i in range(100000):f.write(f'{1 if i<90000 else i-89990},10,20,pv,1511539200\n')
with Lease(config,ROOT/'var/secrets/test_writer.cnf') as lease:
    hdfs=Hdfs(config,lease);remote='/behavior-test/hotspot/'+identity;hdfs.mkdir(remote);hdfs.put(local,remote+'/input.csv')
    with (local.with_suffix('.log')).open('w') as log:
        process=subprocess.Popen(['hadoop','jar',str(ROOT/'jobs/target/behavior-jobs.jar'),'cn.behavior.HotspotAudit',
                    '-Dfs.defaultFS='+config['hdfs']['uri'],remote+'/input.csv',remote+'/outputs'],env=runtime_env(),stdout=log,stderr=log)
        while process.poll() is None:
            import time
            lease.guard();time.sleep(.5)
        assert process.returncode==0,'See '+str(local.with_suffix('.log'))
    result=hdfs.json(remote+'/outputs/verification.json')
    assert result['status']=='PASS' and result['rows']==100000 and result['users']==10001 and result['hot_user_count']==90000
    for app in result['applications']:
        state=application(config,app);assert state['state']=='FINISHED' and state['finalStatus']=='SUCCEEDED'
    atomic_json(ROOT/'var/fixtures/hotspot-verification.json',result)
    print(json.dumps(result))
