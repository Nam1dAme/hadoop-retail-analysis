#!/usr/bin/env python3
"""Audit complete M4 artifacts, populations, validation-only choice and checkpoint scores."""
import hashlib
import json
import math
from pathlib import Path
import sys
import time
import numpy as np
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from orchestration.common import ROOT,load,sha,atomic_json,stamp
from orchestration.control import Lease
from orchestration.storage import Hdfs
from ml.experiment import select
from ml.models import seed_all,make_model,tensors,FEATURES
from ml.metrics import evaluate_file,choose_threshold,SCORE_DTYPE
from ml.shards import ShardDataset
from ml.training import trusted_load

def equivalent(actual,expected):
    if isinstance(expected,dict):
        assert actual.keys()==expected.keys()
        for k in expected:equivalent(actual[k],expected[k])
    elif isinstance(expected,list):
        assert len(actual)==len(expected)
        for a,b in zip(actual,expected):equivalent(a,b)
    elif isinstance(expected,float):assert math.isclose(actual,expected,abs_tol=1e-6,rel_tol=0)
    else:assert actual==expected

def ordered_hash(keys):
    digest=hashlib.sha256()
    for start in range(0,len(keys),65536):
        block=np.ascontiguousarray(keys[start:start+65536]);packed=np.empty((len(block),65),dtype='u1')
        packed[:,:64]=block.view('u1').reshape(-1,64);packed[:,64]=10;digest.update(packed.tobytes())
    return digest.hexdigest()

def main():
    started=time.monotonic();work=ROOT/'var/production';models=json.loads((work/'models.json').read_text());source=json.loads((work/'samples.json').read_text())
    assert models['status']=='SUCCEEDED' and models['experiment_id']==source['experiment_id']
    directory=work/'models'/models['experiment_id'];seed_all()
    with Lease(load(ROOT/'config/development.toml'),ROOT/'var/secrets/writer.cnf') as lease:
        assert Hdfs(lease.config,lease).json(models['archive']+'/manifest.json')==models
        for file in models['outputs']:
            path=directory/file['path'];assert path.resolve().is_relative_to(directory.resolve())
            assert path.stat().st_size==file['bytes'] and sha(path)==file['sha256'],str(path)
        with lease.db.cursor() as q:
            q.execute('SELECT * FROM experiment WHERE experiment_id=%s',(models['experiment_id'],));experiment=q.fetchone()
            assert experiment['status']=='SUCCEEDED' and experiment['test_sealed_at'] is not None
            assert experiment['selected_model_id']==models['selected_model_id']
        candidates=models['selection']['candidates'];assert len(candidates)==5
        assert [x['algorithm'] for x in candidates]==['CONSTANT','LOGISTIC','LOGISTIC','GRU','GRU']
        validation=ShardDataset(Path(source['samples']['cache_path'])/'VALIDATION',verify=True)
        test_meta=source['samples']['splits']['TEST'];validated=[]
        for item in candidates:
            lease.guard();algorithm=item['algorithm'];path=Path(item['validation_scores']);data=np.load(path,mmap_mode='r',allow_pickle=False)
            assert data.dtype==SCORE_DTYPE and len(data)==validation.manifest['rows']
            assert ordered_hash(data['sample_key'])==validation.manifest['sample_order_sha256']
            assert int((data['label']==1).sum())==validation.manifest['positive_count']
            assert choose_threshold(data['score'],data['label'])==item['threshold']
            equivalent(evaluate_file(path,item['threshold']),item['validation_metrics'])
            if algorithm=='CONSTANT':
                rules=json.loads(Path(item['checkpoint_path']).read_text());assert rules['rate']==source['samples']['splits']['TRAIN']['positive_count']/source['samples']['splits']['TRAIN']['rows']
                assert np.all(data['score']==rules['rate']) and item['parameter_count']==0
                checkpoint_error=0.
            else:
                assert item['config']['learning_rate'] in (.001,.0003)
                device=torch.device('cuda' if algorithm=='GRU' else 'cpu')
                model=make_model(algorithm).to(device);state=trusted_load(item['checkpoint_path'],item['checkpoint_hash'],directory)
                model.load_state_dict(state);model.eval();assert sum(p.numel() for p in model.parameters())==item['parameter_count']
                batch=next(validation.batches(64,shuffle=False));inputs=tensors(batch,device,algorithm)
                with torch.no_grad():actual=torch.sigmoid(model(inputs).float()).cpu().numpy()
                checkpoint_error=float(np.max(np.abs(actual-data['score'][:64])));assert checkpoint_error<=1e-6
                if algorithm=='GRU':
                    assert item['hardware']['device']=='cuda' and '4060 Laptop' in item['hardware']['name']
                    assert torch.count_nonzero(model.category.weight[0])==0
                    assert item['peak_allocated_mib']<=6144 and item['peak_reserved_mib']<=6656
                assert item['rss_mib']<=3072 and item['training_seconds']<=8*3600
                with lease.db.cursor() as q:
                    q.execute('SELECT epoch,sample_count FROM model_epoch WHERE model_run_id=%s ORDER BY epoch',(item['model_run_id'],));epochs=q.fetchall()
                assert [x['epoch'] for x in epochs]==list(range(1,len(epochs)+1)) and 1<=len(epochs)<=10
                assert all(x['sample_count']==source['samples']['splits']['TRAIN']['rows'] for x in epochs)
                del model
                if device.type=='cuda':torch.cuda.empty_cache()
            validated.append(dict(model_run_id=item['model_run_id'],algorithm=algorithm,validation_ap=item['validation_metrics']['ALL']['ap'],checkpoint_error=checkpoint_error))
        finalists=[select([x for x in candidates if x['algorithm']==name]) for name in ('CONSTANT','LOGISTIC','GRU')]
        assert [x['model_run_id'] for x in finalists]==[x['model_run_id'] for x in models['selection']['finalists']]
        assert select(finalists)['model_run_id']==models['selected_model_id']
        for item in models['test']:
            data=np.load(item['scores'],mmap_mode='r',allow_pickle=False)
            assert sha(item['scores'])==item['sha256'] and len(data)==test_meta['rows']
            assert int((data['label']==1).sum())==test_meta['positive_count']
            assert ordered_hash(data['sample_key'])==test_meta['sample_order_sha256']
            equivalent(evaluate_file(item['scores'],item['threshold']),item['metrics'])
        benchmark=json.loads((directory/'benchmark.json').read_text());assert benchmark['status']=='PASS' and len(benchmark['records'])==6
        assert all(r['steps']==200 and r['processed_samples']==12800 for r in benchmark['records'])
        assert max(benchmark['first_loss_differences'])<=1e-4
        recovery=json.loads((work/'cuda-recovery-test.json').read_text());assert recovery['status']=='PASS'
        report=dict(status='PASS',experiment_id=models['experiment_id'],selected_model_id=models['selected_model_id'],
                    validation=validated,test=[dict(algorithm=x['algorithm'],metrics=x['metrics']) for x in models['test']],
                    benchmark_speedup=benchmark['speedup'],cuda_recovery='PASS',verified_files=len(models['outputs']),
                    verification_seconds=time.monotonic()-started,verified_at=stamp())
        atomic_json(work/'model-verification.json',report);print(json.dumps(report))

if __name__=='__main__':main()
