import json
from pathlib import Path
import time
import torch
from orchestration.common import ROOT,atomic_json,sha,fingerprint,ManifestError,stamp
from orchestration.pipeline import stage_run
from ml.models import make_model
from ml.training import score,trusted_load
from ml.shards import ShardDataset


def forecast(lease,index=None):
    index=Path(index or ROOT/'var/production/models.json')
    models=json.loads(index.read_text());sample=json.loads((ROOT/'var/production/samples.json').read_text())
    if models['status']!='SUCCEEDED' or sample['experiment_id']!=models['experiment_id']:
        raise ManifestError('Prediction requires a completed matching experiment')
    selected=next(x for x in models['selection']['finalists'] if x['model_run_id']==models['selected_model_id'])
    with lease.db.cursor() as q:
        q.execute('SELECT status,selected_model_id,sample_hash FROM experiment WHERE experiment_id=%s',(models['experiment_id'],));row=q.fetchone()
        if not row or row['status']!='SUCCEEDED' or row['selected_model_id']!=selected['model_run_id'] or row['sample_hash']!=models['sample_hash']:
            raise ManifestError('Experiment selection differs')
    local=ROOT/'var/production/predictions'/models['experiment_id'];local.mkdir(parents=True,exist_ok=True)
    algorithm=selected['algorithm'];device=torch.device('cuda' if algorithm=='GRU' else 'cpu')
    def produce(storage,temp,work,run):
        started=time.monotonic();data=ShardDataset(Path(sample['samples']['cache_path'])/'FORECAST',verify=True)
        model=None
        if algorithm!='CONSTANT':
            model=make_model(algorithm).to(device)
            model.load_state_dict(trusted_load(selected['checkpoint_path'],selected['checkpoint_hash'],ROOT/'var/production/models'))
        scores=score(data,model,device,algorithm,local/'forecast.npy',rate=selected['config'].get('rate'),lease=lease)
        import numpy as np
        arr=np.load(local/'forecast.npy',mmap_mode='r',allow_pickle=False)
        if not (arr['label']==-1).all() or len(arr)!=sample['samples']['splits']['FORECAST']['rows']:
            raise ManifestError('Forecast population or unobserved labels differ')
        result=dict(status='SUCCEEDED',experiment_id=models['experiment_id'],batch_id=models['batch_id'],model_run_id=selected['model_run_id'],
                    algorithm=algorithm,checkpoint_hash=selected['checkpoint_hash'],threshold=selected['threshold'],
                    as_of='2017-12-03T16:00:00Z',window_end='2017-12-04T16:00:00Z',
                    scores=scores,rows=scores['rows'],labels='UNOBSERVED',seconds=time.monotonic()-started,created_at=stamp())
        atomic_json(local/'forecast.json',result)
        storage.put(local/'forecast.npy',temp+'/forecast.npy');storage.put(local/'forecast.json',temp+'/forecast.json')
        del model
        if device.type=='cuda':torch.cuda.empty_cache()
        return {'forecast':result}
    params=dict(experiment_id=models['experiment_id'],model_run_id=selected['model_run_id'],checkpoint_hash=selected['checkpoint_hash'],
                threshold=selected['threshold'],implementation=fingerprint({p.name:sha(p) for p in sorted((ROOT/'publication').glob('*.py'))}))
    result=stage_run(lease,models['batch_id'],'P01',[dict(path=str(index),sha256=sha(index)),dict(path=str(ROOT/'var/production/samples.json'),sha256=sha(ROOT/'var/production/samples.json'))],params,produce)
    atomic_json(ROOT/'var/production/forecast.json',result)
    return result
