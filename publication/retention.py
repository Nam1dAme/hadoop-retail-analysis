"""Read-only, reference-protected retention planning. This module never deletes data."""
from datetime import timedelta
import json
from pathlib import Path, PurePosixPath
from orchestration.common import ROOT, now, stamp, ManifestError
from orchestration.storage import Hdfs


def protected_path(path, roots):
    p = PurePosixPath(path)
    if not p.is_absolute() or '..' in p.parts:
        raise ManifestError('Unsafe retention path')
    return any(p == PurePosixPath(root) or PurePosixPath(root) in p.parents for root in roots)


def _entry_bytes(path):
    p = Path(path)
    if not p.exists():
        return 0
    if p.is_file() or p.is_symlink():
        return p.stat().st_size
    return sum(f.stat().st_size for f in p.rglob('*') if f.is_file())


def _scan_hdfs_tmp(config, active_runs, protected, candidates, blocked, errors):
    hdfs_root = config['hdfs']['root']
    try:
        storage = Hdfs(config)
        out = storage.command('-find', hdfs_root, '-name', '*.tmp').stdout.decode('utf-8', errors='replace')
    except Exception as exc:
        errors.append(dict(kind='hdfs', scope=hdfs_root, error=str(exc)))
        return
    seen = {c['path'] for c in candidates} | {b['path'] for b in blocked}
    for line in out.splitlines():
        path = line.strip()
        if not path or path in seen:
            continue
        name = PurePosixPath(path).name
        if not (name.startswith('.source-') or name.endswith('.tmp')):
            continue
        if any(run_id in name for run_id in active_runs):
            blocked.append(dict(path=path, kind='hdfs', reason='active task temporary'))
            continue
        if not path.startswith(hdfs_root + '/'):
            raise ManifestError('Temporary path escaped project HDFS root')
        if protected_path(path, protected):
            blocked.append(dict(path=path, kind='hdfs', reason='referenced or active'))
        else:
            candidates.append(dict(path=path, kind='hdfs', reason='orphaned HDFS temporary'))


def _scan_local_bloat(protected, model_runs, candidates, blocked):
    def add_local(path, reason):
        s = str(path)
        if protected_path(s, protected):
            blocked.append(dict(path=s, kind='local', reason='referenced or active'))
        else:
            candidates.append(dict(path=s, kind='local', bytes=_entry_bytes(path), reason=reason))

    for subdir, reason in (
        ('var/recovery', 'orphaned recovery directory'),
        ('var/scale', 'scale verification artifact'),
        ('var/downloads', 'cached download artifact'),
    ):
        base = ROOT / subdir
        if base.is_dir():
            for item in sorted(base.iterdir()):
                add_local(item, reason)

    pre_mig_dir = ROOT / 'var/production/backups'
    if pre_mig_dir.is_dir():
        for item in sorted(pre_mig_dir.glob('pre-migration-*.sql')):
            add_local(item, 'pre-migration SQL dump')

    backups_dir = ROOT / 'var/backups'
    if backups_dir.is_dir():
        releases = sorted(
            (p for p in backups_dir.glob('release-*') if p.is_dir()),
            key=lambda p: (p.stat().st_mtime, p.name),
            reverse=True,
        )
        for old in releases[1:]:
            add_local(old, 'superseded release backup')

    models_dir = ROOT / 'var/production/models'
    if models_dir.is_dir():
        by_run = {r['model_run_id']: r for r in model_runs}
        for exp_dir in sorted(models_dir.iterdir()):
            if not exp_dir.is_dir():
                continue
            for run_dir in sorted(exp_dir.iterdir()):
                if not run_dir.is_dir():
                    continue
                row = by_run.get(run_dir.name)
                if row is None:
                    continue
                keep = set()
                if row.get('checkpoint_path'):
                    keep.add(Path(row['checkpoint_path']).resolve())
                if row.get('best_epoch') is not None:
                    keep.add((run_dir / f"epoch-{int(row['best_epoch']):02}-validation.npy").resolve())
                if row.get('status') in ('RUNNING', 'PENDING') and row.get('resume_path'):
                    keep.add(Path(row['resume_path']).resolve())
                for pattern in ('epoch-*-resume.pt', 'epoch-*-state.pt', 'epoch-*-validation.npy'):
                    for item in sorted(run_dir.glob(pattern)):
                        if item.resolve() not in keep:
                            candidates.append(dict(
                                path=str(item),
                                kind='local',
                                bytes=item.stat().st_size,
                                reason='superseded ML epoch artifact',
                            ))


def plan(db, config):
    """只读计划：分类可清理候选与受保护引用，不提供也不执行任何删除。"""
    protected = {config['source']['path']}
    candidates = []
    blocked = []
    errors = []
    active_runs = set()
    model_runs = []
    with db.cursor() as q:
        q.execute('SELECT release_id FROM current_release WHERE singleton_id=1')
        current = q.fetchone()['release_id']
        # Keep every READY/STAGING version, exceeding the minimum of current + previous READY.
        q.execute("SELECT release_id,batch_id,experiment_id,manifest_json FROM data_release WHERE status IN ('READY','STAGING')")
        releases = q.fetchall()
        batches = {r['batch_id'] for r in releases}
        for r in releases:
            manifest = json.loads(r['manifest_json'])
            for key in ('models_archive', 'forecast_archive'):
                if manifest.get(key):
                    protected.add(manifest[key])
            protected.add(str(ROOT / 'var/production/models' / r['experiment_id']))
        q.execute('SELECT run_id,batch_id,status,output_path,finished_at FROM task_run')
        tasks = q.fetchall()
        for task in tasks:
            if task['status'] in ('RUNNING', 'PENDING'):
                active_runs.add(task['run_id'])
            if task['status'] in ('RUNNING', 'PENDING') or (task['status'] == 'SUCCEEDED' and task['batch_id'] in batches):
                protected.add(task['output_path'])
                protected.add(str(ROOT / 'var/production/runs' / task['run_id']))
        # The unique source archive is always retained, even after a local-only backup.
        q.execute('SELECT batch_id,manifest_path,source_sha256 FROM data_batch')
        sources = q.fetchall()
        for source in sources:
            protected.add(config['hdfs']['root'] + '/raw/source_sha256=' + source['source_sha256'])
            if source['manifest_path']:
                protected.add(str(PurePosixPath(source['manifest_path']).parent))
        q.execute('SELECT experiment_id,sample_manifest FROM experiment')
        experiments = q.fetchall()
        for exp in experiments:
            if exp['sample_manifest'].startswith('/'):
                protected.add(str(PurePosixPath(exp['sample_manifest']).parent))
        try:
            q.execute('SELECT model_run_id,status,checkpoint_path,resume_path,best_epoch FROM model_run')
            model_runs = q.fetchall()
        except Exception:
            model_runs = []
    for task in tasks:
        if task['status'] != 'FAILED' or not task['finished_at'] or task['finished_at'] > now() - timedelta(days=7):
            continue
        final = PurePosixPath(task['output_path'])
        temporary = str(final.parent / ('.' + task['run_id'] + '.tmp'))
        if not temporary.startswith(config['hdfs']['root'] + '/'):
            raise ManifestError('Temporary path escaped project HDFS root')
        if protected_path(temporary, protected):
            blocked.append(dict(path=temporary, kind='hdfs', reason='referenced or active'))
        else:
            candidates.append(dict(path=temporary, kind='hdfs', run_id=task['run_id'], reason='failed temporary older than seven days'))
    _scan_hdfs_tmp(config, active_runs, protected, candidates, blocked, errors)
    _scan_local_bloat(protected, model_runs, candidates, blocked)
    return dict(
        status='PASS',
        mode='DRY_RUN',
        dry_run=True,
        deletion_authorized=False,
        current_release_id=current,
        retained_release_ids=[r['release_id'] for r in releases],
        protected_roots=sorted(protected),
        candidates=candidates,
        blocked=blocked,
        errors=errors,
        unique_source_reclaim_allowed=False,
        policy='Read-only plan; keep all READY/STAGING dependencies and recheck live tasks before any future deletion.',
        observed_at=stamp(),
    )
