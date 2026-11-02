"""Materialize sealed calibration bins from verified model artifacts for the read API."""
import json
from pathlib import Path
from orchestration.common import ROOT,sha,ManifestError

def materialize_calibration(lease):
    manifest=json.loads((ROOT/'var/production/models.json').read_text())
    directory=ROOT/'var/production/models'/manifest['experiment_id']
    expected={f['path']:f['sha256'] for f in manifest['outputs']}
    candidates=manifest['selection']['candidates']
    for item in candidates:
        for split,name in [('VALIDATION','validation_metrics.json'),('TEST','test_metrics.json')]:
            path=directory/item['model_run_id']/name
            if not path.exists():
                if split=='TEST' and item['model_run_id'] not in {x['model_run_id'] for x in manifest['selection']['finalists']}:continue
                raise ManifestError('Missing sealed model metrics')
            if sha(path)!=expected.get(str(path.relative_to(directory))):raise ManifestError('Changed model metric artifact')
            result=json.loads(path.read_text())
            score_path=Path(item['validation_scores']) if split=='VALIDATION' else directory/item['model_run_id']/'test.npy'
            evaluation_hash=sha(score_path)
            with lease.db.cursor() as q:
                for cohort,metrics in result.items():
                    bins=metrics['calibration']
                    if metrics['sample_count'] and (len(bins)!=10 or sum(b['count'] for b in bins)!=metrics['sample_count']):
                        raise ManifestError('Calibration population differs')
                    for index,bin in enumerate(bins):
                        for key in ('count','mean_score','observed_rate'):
                            q.execute('INSERT INTO model_metric VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE value=VALUES(value),evaluation_hash=VALUES(evaluation_hash)',
                                      (item['model_run_id'],split,cohort,f'calibration_{index}_{key}',bin[key],metrics['sample_count'],metrics['positive_count'],metrics.get('reason'),evaluation_hash))
