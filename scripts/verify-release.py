#!/usr/bin/env python3
"""Compare every stored prediction with original ID/anchor/label/score using disk runs."""
import hashlib
import heapq
import json
from pathlib import Path
import sys
import tempfile
import time
import numpy as np
import pymysql
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,atomic_json,stamp,fingerprint
from orchestration.control import Lease
from orchestration.database import connect

ROW=np.dtype([('user','S20'),('score','<f8'),('label','i1'),('history','<i8'),('key','S64'),('asof','<i8')])

def expected_rows(cache,scores,directory):
    manifest=json.loads((cache/'manifest.json').read_text());prediction=np.load(scores,mmap_mode='r',allow_pickle=False);offset=0;runs=[]
    for shard in manifest['shards']:
        root=cache/shard['directory'];n=shard['rows'];data=np.empty(n,dtype=ROW)
        ids=(root/'user_ids.utf8').read_bytes();positions=np.load(root/'user_offsets.npy',mmap_mode='r',allow_pickle=False)
        asof=np.load(root/'as_of.npy',mmap_mode='r',allow_pickle=False);keys=np.load(root/'sample_key.npy',mmap_mode='r',allow_pickle=False)
        label=np.load(root/'label.npy',mmap_mode='r',allow_pickle=False)
        history=np.load(root/'history_event_count.npy',mmap_mode='r',allow_pickle=False)
        part=prediction[offset:offset+n];assert np.array_equal(part['sample_key'],keys) and np.array_equal(part['label'],label)
        for i in range(n):
            user=ids[positions[i]:positions[i+1]];data['user'][i]=user
            assert hashlib.sha256(user+b'|'+str(int(asof[i])).encode()).hexdigest().encode()==keys[i]
        data['score']=part['score'];data['label']=label;data['history']=history;data['key']=keys;data['asof']=asof
        data.sort(order='user');path=directory/f'{len(runs):04}.npy';np.save(path,data,allow_pickle=False)
        runs.append(np.load(path,mmap_mode='r',allow_pickle=False));offset+=n
    assert offset==len(prediction)
    heap=[(bytes(run[0]['user']),i,0) for i,run in enumerate(runs) if len(run)];heapq.heapify(heap)
    previous=None
    while heap:
        user,run,index=heapq.heappop(heap)
        assert previous is None or user>previous,'Duplicate sample user in one prediction window'
        previous=user;yield runs[run][index];index+=1
        if index<len(runs[run]):heapq.heappush(heap,(bytes(runs[run][index]['user']),run,index))

def main():
    started=time.monotonic();work=ROOT/'var/production';release=json.loads((work/'release.json').read_text());models=json.loads((work/'models.json').read_text())
    forecast=json.loads((work/'forecast.json').read_text())['forecast'];samples=json.loads((work/'samples.json').read_text());config=load(ROOT/'config/development.toml')
    sources=[('TEST',m['model_run_id'],m['scores'],m['threshold']) for m in models['test']]+[('FORECAST',forecast['model_run_id'],forecast['scores']['path'],forecast['threshold'])]
    verified=[]
    with Lease(config,ROOT/'var/secrets/writer.cnf') as lease:
        with lease.db.cursor() as q:
            q.execute('SELECT * FROM data_release WHERE release_id=%s',(release['release_id'],));published=q.fetchone()
            assert published['status']=='READY' and published['manifest_hash']==release['manifest_hash']
            assert fingerprint(json.loads(published['manifest_json']))==published['manifest_hash']
        for mode,model,path,threshold in sources:
            with tempfile.TemporaryDirectory(dir=work,prefix='prediction-audit-') as directory,connect(config,ROOT/'var/secrets/reader.cnf') as db:
                db._read_timeout=300
                expected=expected_rows(Path(samples['samples']['cache_path'])/mode,path,Path(directory))
                count=0
                with db.cursor(pymysql.cursors.SSCursor) as q:
                    q.execute('SELECT user_id,score,label,history_event_count,sample_key,UNIX_TIMESTAMP(as_of),UNIX_TIMESTAMP(window_end),predicted_positive FROM prediction WHERE release_id=%s AND mode=%s AND model_run_id=%s ORDER BY user_id',
                              (release['release_id'],mode,model))
                    for row in q:
                        item=next(expected)
                        assert row[0].encode()==item['user'] and row[1]==float(item['score'])
                        assert row[2]==(None if mode=='FORECAST' else int(item['label']))
                        assert row[3]==int(item['history']) and row[4].encode()==item['key']
                        assert int(row[5])==int(item['asof']) and int(row[6])==int(item['asof'])+86400
                        assert bool(row[7])==(row[1]>=threshold)
                        count+=1
                        if count%100000==0:lease.guard();print(json.dumps(dict(mode=mode,model=model,verified_rows=count)),flush=True)
                assert next(expected,None) is None
                assert count==samples['samples']['splits'][mode]['rows']
                verified.append(dict(mode=mode,model_run_id=model,rows=count))
        report=dict(status='PASS',release_id=release['release_id'],predictions=verified,total_verified_rows=sum(x['rows'] for x in verified),
                    independent_backup_verified=False,seconds=time.monotonic()-started,verified_at=stamp())
        atomic_json(work/'release-verification.json',report);print(json.dumps(report))

if __name__=='__main__':main()
