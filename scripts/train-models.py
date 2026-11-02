#!/usr/bin/env python3
"""Run the complete M4 experiment with a global heavy-task lease."""
import argparse
from pathlib import Path
import subprocess
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT, load, encode
from orchestration.database import require_current
from orchestration.control import Lease
from ml.experiment import run_experiment

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', type=Path, default=ROOT/'var/production/samples.json')
    args = parser.parse_args()
    if subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip():
        raise SystemExit('Commit implementation changes before formal training')
    config = load(ROOT/'config/development.toml')
    with Lease(config, ROOT/'var/secrets/writer.cnf') as lease:
        require_current(lease.db)
        result = run_experiment(lease, args.samples)
        print(encode({'status': result['status'], 'experiment_id': result['experiment_id'], 'selected_model_id': result['selected_model_id']}))

if __name__ == '__main__':
    main()
