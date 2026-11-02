#!/usr/bin/env python3
"""Read-only reference closure and aged temporary candidates; no delete option."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,encode
from orchestration.database import connect
from publication.retention import plan
config=load(ROOT/'config/development.toml')
with connect(config,ROOT/'var/secrets/reader.cnf') as db:result=plan(db,config)
atomic_json(ROOT/'var/production/retention-plan.json',result)
print(encode(result))
