#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,encode
from orchestration.database import require_current,connect
from orchestration.control import Lease
from publication.forecast import forecast
from publication.release import publish,publish_pointer
from publication.reporting import materialize_calibration

p=argparse.ArgumentParser();p.add_argument('action',choices=['forecast','publish','rollback']);p.add_argument('--release-id');p.add_argument('--expected-generation',type=int);args=p.parse_args()
if subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip():raise SystemExit('Commit changes before formal publication')
with Lease(load(ROOT/'config/development.toml'),ROOT/'var/secrets/writer.cnf') as lease:
 require_current(lease.db)
 if args.action=='forecast':result=forecast(lease)
 else:
  if args.action=='publish':materialize_calibration(lease)
  with connect(lease.config,ROOT/'var/secrets/loader.cnf') as result_db:
   result_lease=SimpleNamespace(config=lease.config,db=result_db,guard=lease.guard)
   if args.action=='publish':result=publish(result_lease)
   else:
    import uuid
    uuid.UUID(args.release_id)
    if args.expected_generation is None:raise SystemExit('--expected-generation is required for rollback')
    publish_pointer(result_db,args.release_id,args.expected_generation,action='ROLLBACK');result={'status':'SUCCEEDED','release_id':args.release_id}
 print(encode(result))
