#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,encode
from orchestration.database import require_current
from orchestration.control import Lease
from publication.forecast import forecast
from publication.release import publish,publish_pointer

p=argparse.ArgumentParser();p.add_argument('action',choices=['forecast','publish','rollback']);p.add_argument('--release-id');p.add_argument('--expected-generation',type=int);args=p.parse_args()
if subprocess.check_output(['git','status','--porcelain'],cwd=ROOT).strip():raise SystemExit('Commit changes before formal publication')
with Lease(load(ROOT/'config/development.toml'),ROOT/'var/secrets/writer.cnf') as lease:
 require_current(lease.db)
 if args.action=='forecast':result=forecast(lease)
 elif args.action=='publish':result=publish(lease)
 else:
  import uuid
  uuid.UUID(args.release_id)
  if args.expected_generation is None:raise SystemExit('--expected-generation is required for rollback')
  publish_pointer(lease.db,args.release_id,args.expected_generation,action='ROLLBACK');result={'status':'SUCCEEDED','release_id':args.release_id}
 print(encode(result))
