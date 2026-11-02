#!/usr/bin/env python3
"""Actual range ranking: every date range plus immutable Hadoop Top100 and HTTP cursors."""
from datetime import date,timedelta
import json
from pathlib import Path
import sys
import time
import urllib.request
import urllib.parse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,stamp,commit,sha,fingerprint
from orchestration.database import require_current
from orchestration.control import Lease
from orchestration.storage import Hdfs
from orchestration.pipeline import validate
from service.database import Pool
from service.ranking import rank_entities

class Traced:
    def __init__(self,pool):self.pool=pool;self.queries=0;self.transferred_rows=0
    def all(self,*args,**kwargs):
        self.queries+=1;rows=self.pool.all(*args,**kwargs);self.transferred_rows+=len(rows);return rows

def main():
    started=time.monotonic();config=load(ROOT/'config/development.toml');release=json.loads((ROOT/'var/production/release.json').read_text())
    pipeline=json.loads((ROOT/'var/production/pipeline.json').read_text());rid=release['release_id'];results=[];current=None;failure=None
    with Lease(config,ROOT/'var/secrets/writer.cnf') as lease:
        require_current(lease.db);run=pipeline['stages']['J03rank']['run_id']
        local=json.loads((ROOT/'var/production/runs'/run/'manifest.json').read_text());storage=Hdfs(config,lease)
        manifest=validate(storage,local['output_path'],{'run_id':run,'batch_id':pipeline['batch_id']})
        golden={}
        for line in storage.dump([x for x in manifest['outputs'] if Path(x['path']).name.startswith('part-')]):
            day,kind,behavior,rank,entity,count=line.split('\t')
            golden.setdefault((day,kind,behavior),[]).append(dict(rank=int(rank),entity_id=entity,event_count=int(count)))
        for rows in golden.values():rows.sort(key=lambda row:row['rank'])
        assert len(golden)==80
        pool=Traced(Pool(config,ROOT/'var/secrets/reader.cnf'));begin=date(2017,11,25);end=date(2017,12,4)
        def evaluate(start,stop,kind,behavior,limit,expected=None):
            nonlocal current
            lease.guard();current=dict(start=str(start),end=str(stop),entity_type=kind,behavior=behavior,limit=limit)
            tick=time.monotonic();queries=pool.queries;transferred=pool.transferred_rows
            rows=rank_entities(pool,rid,start,stop,kind,behavior,limit)
            assert rows==sorted(rows,key=lambda r:(-r['event_count'],r['entity_id'])) and len({r['entity_id'] for r in rows})==len(rows)
            if expected is not None:assert rows==[{k:r[k] for k in ('entity_id','event_count')} for r in expected]
            results.append(dict(**current,seconds=time.monotonic()-tick,rows=len(rows),queries=pool.queries-queries,transferred_rows=pool.transferred_rows-transferred,
                                hadoop_oracle=expected is not None,result_hash=fingerprint(rows)))
        http_pages=0
        try:
            # All 45 valid date ranges, both entity types and all four behavior types.
            for left in range(9):
                for right in range(left+1,10):
                    for kind in ('ITEM','CATEGORY'):
                        for behavior in ('pv','buy','cart','fav'):
                            start=begin+timedelta(days=left);stop=begin+timedelta(days=right)
                            key=('ALL' if left==0 and right==9 else str(start),kind,behavior)
                            expected=golden[key][:21] if right-left==1 or (left==0 and right==9) else None
                            evaluate(start,stop,kind,behavior,21,expected)
            for (day,kind,behavior),expected in sorted(golden.items()):
                start=begin if day=='ALL' else date.fromisoformat(day);stop=end if day=='ALL' else start+timedelta(days=1)
                evaluate(start,stop,kind,behavior,len(expected),expected)
                params=dict(release_id=rid,start=str(start),end=str(stop),entity_type=kind,behavior=behavior,limit=20)
                received=[]
                for _ in range(2):
                    with urllib.request.urlopen('http://127.0.0.1:8010/api/v1/entities?'+urllib.parse.urlencode(params),timeout=7) as r:value=json.load(r)
                    assert value['meta']['release_id']==rid
                    received.extend(value['data']);http_pages+=1
                    if not value['meta']['next_cursor']:break
                    params['cursor']=value['meta']['next_cursor']
                assert received==[dict(row,event_count=str(row['event_count'])) for row in expected[:len(received)]]
                assert len(received)==min(40,len(expected))
        except Exception as error:
            failure=dict(case=current,error_type=type(error).__name__,message=str(error))
        result=dict(status='FAIL' if failure else 'PASS',baseline_version='UBA-DOC-1.0.0',code_commit=commit(),environment_sha256=sha(ROOT/'config/environment.lock.json'),
                    release_id=rid,hadoop_run_id=run,hadoop_manifest_hash=fingerprint(manifest),hadoop_groups=len(golden),
                    expected_range_checks=360,expected_oracle_checks=80,completed_checks=len(results),http_pages=http_pages,
                    results=results,failure=failure,seconds=time.monotonic()-started,verified_at=stamp(),
                    scope='Exact 45-range SQL results and immutable Hadoop daily/full-window Top100; HTTP first two pages; not the ten-concurrency SLA')
        atomic_json(ROOT/'var/production/entity-ranking-verification.json',result)
        print(json.dumps({k:v for k,v in result.items() if k!='results'}))
        if failure:raise SystemExit(1)

if __name__=='__main__':main()
