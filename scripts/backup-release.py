#!/usr/bin/env python3
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,encode
from orchestration.control import Lease
from publication.backup import make_backup

with Lease(load(ROOT/'config/development.toml'),ROOT/'var/secrets/writer.cnf') as lease:
    result=make_backup(lease)
    print(encode({k:v for k,v in result.items() if k!='hdfs_files'}))
