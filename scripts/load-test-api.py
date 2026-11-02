#!/usr/bin/env python3
"""NF-05: ten concurrent readers for five minutes, with cold timings kept separate."""
from array import array
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
import subprocess
import time
import urllib.request
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,stamp
from orchestration.control import Lease

def main():
    base='http://127.0.0.1:8010/api/v1'
    release=json.loads((ROOT/'var/production/release.json').read_text())['release_id']
    def read(route,params):
        started=time.perf_counter()
        with urllib.request.urlopen(base+route+'?'+urllib.parse.urlencode(params),timeout=10) as response:
            result=json.load(response)
        if result['meta']['release_id']!=release:raise ValueError('Release switched inside benchmark')
        return time.perf_counter()-started,result
    scenarios=[('/overview',{}),('/trends',{'granularity':'day'}),('/activity',{}),
               ('/entities',{'entity_type':'ITEM','behavior':'pv','limit':20}),
               ('/predictions',{'mode':'FORECAST','limit':20}),('/tasks',{'limit':20})]
    cold=[]
    with Lease(load(ROOT/'config/development.toml'),ROOT/'var/secrets/writer.cnf') as lease:
        # Cold timings refer to a fresh API process cache, not earlier browser-warmed data.
        with (ROOT/'var/production/load-test-restart.log').open('w') as log:
            for action in ('stop','start'):
                subprocess.run([str(ROOT/'.venv/bin/python'),'scripts/web-service.py',action],cwd=ROOT,stdout=log,stderr=log,check=True)
        for route,params in list(scenarios):
            params['release_id']=release
            elapsed,value=read(route,params);cold.append(dict(route=route,seconds=elapsed))
            if route in ('/entities','/predictions') and value['meta']['next_cursor']:
                next_params=dict(params,cursor=value['meta']['next_cursor']);read(route,next_params);scenarios.append((route,next_params))
        for _,params in scenarios:params['release_id']=release
        lock=threading.Lock();latencies=array('d');errors=[];error_count=0;counts={}
        started=time.monotonic();deadline=started+300
        def worker(index):
            nonlocal error_count
            step=index
            while time.monotonic()<deadline:
                route,params=scenarios[step%len(scenarios)];step+=1
                tick=time.perf_counter();ok=False
                try:elapsed,_=read(route,params);ok=True
                except Exception as error:
                    elapsed=time.perf_counter()-tick
                    with lock:
                        error_count+=1
                        if len(errors)<20:errors.append(dict(route=route,error=type(error).__name__))
                with lock:
                    latencies.append(elapsed);counts[route]=counts.get(route,0)+1
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures=[pool.submit(worker,i) for i in range(10)]
            while not all(f.done() for f in futures):
                lease.guard();time.sleep(.5)
            for future in futures:future.result()
        elapsed=time.monotonic()-started;ordered=sorted(latencies)
        p95=ordered[min(len(ordered)-1,int(len(ordered)*.95))]
        error_rate=error_count/len(ordered)
        report=dict(status='PASS' if p95<=2 and error_rate<.01 and elapsed>=300 else 'FAIL',
                    release_id=release,concurrency=10,duration_seconds=elapsed,requests=len(ordered),
                    p95_seconds=p95,error_rate=error_rate,errors=errors,requests_by_route=counts,
                    cold_timings=cold,observed_at=stamp())
        atomic_json(ROOT/'var/production/api-load-test.json',report)
        print(json.dumps(report))
        if report['status']!='PASS':raise SystemExit(1)

if __name__=='__main__':main()
