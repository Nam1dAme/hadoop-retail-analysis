#!/usr/bin/env python3
"""Read-only sampled telemetry. Gaps and observation start remain explicit."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]
stop=False
def finish(*args):
    global stop
    stop=True
signal.signal(signal.SIGTERM,finish);signal.signal(signal.SIGINT,finish)
p=argparse.ArgumentParser();p.add_argument('--interval',type=float,default=5);p.add_argument('--output',type=Path,default=ROOT/'var/production/resource-telemetry.jsonl');args=p.parse_args()
args.output.parent.mkdir(parents=True,exist_ok=True)
with args.output.open('a',buffering=1) as out:
    while not stop:
        started=time.monotonic();mem={k.rstrip(':'):int(v.split()[0])*1024 for k,v in (line.split(None,1) for line in Path('/proc/meminfo').read_text().splitlines())}
        record=dict(observed_at=datetime.now(timezone.utc).isoformat(),mem_available=mem['MemAvailable'],swap_used=mem['SwapTotal']-mem['SwapFree'],linux_free=shutil.disk_usage(ROOT).free)
        try:
            result=subprocess.run(['/usr/lib/wsl/lib/nvidia-smi','--query-gpu=memory.total,memory.used,utilization.gpu,temperature.gpu,power.draw','--format=csv,noheader,nounits'],capture_output=True,text=True,timeout=3)
            if result.returncode:raise OSError('nvidia-smi returned nonzero')
            values=[float(x.strip()) if x.strip() not in ('[N/A]','N/A','[Not Supported]') else None for x in result.stdout.split(',')]
            record['gpu']=dict(zip(('total_mib','used_mib','utilization_percent','temperature_c','power_watts'),values))
        except (OSError,ValueError,subprocess.TimeoutExpired):record['gpu']=None
        for label,pid_file in [('database',ROOT/'var/mariadb/server.pid'),('api',ROOT/'var/run/web/api.json'),('ui',ROOT/'var/run/web/ui.json')]:
            try:
                raw=pid_file.read_text();pid=int(json.loads(raw)['pid']) if pid_file.suffix=='.json' else int(raw)
                status=Path(f'/proc/{pid}/status').read_text();record[label+'_rss_bytes']=int(next(x.split()[1] for x in status.splitlines() if x.startswith('VmRSS:')))*1024
            except (OSError,ValueError,KeyError,StopIteration):record[label+'_rss_bytes']=None
        out.write(json.dumps(record,separators=(',',':'))+'\n')
        deadline=started+args.interval
        while not stop and time.monotonic()<deadline:time.sleep(min(.2,max(0,deadline-time.monotonic())))
