#!/usr/bin/env python3
"""手动运行的新旧 J01/J02 等价验证（不属于默认 pytest 集合）。

只用 LocalJobRunner + 本地文件系统；不启动 HDFS/YARN 守护进程。

    .venv/bin/python tests/verify_local_equivalence.py

旧 jar 从 HEAD 的 jobs 源码在临时 git worktree 中构建，新 jar 从当前工作树构建。
比较：业务 counters、按逻辑目录归组并排序后的记录、J02 汇总行、Python 侧求和结果。
"""
import argparse
import hashlib
import json
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime
import zlib
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from orchestration.common import dataset_window, runtime_env  # noqa: E402

ENV = runtime_env()
WINDOW = dataset_window()
START_DATE = WINDOW['start'].isoformat()
DAYS = WINDOW['days']
BASE_TS = int(datetime.combine(WINDOW['start'], datetime.min.time(),
                               tzinfo=ZoneInfo('Asia/Shanghai')).timestamp())
END_TS = BASE_TS + DAYS * 86400
SOURCE_SHA = None
FRAMEWORK_GROUPS = {'Shuffle Errors', 'File Input Format Counters', 'File Output Format Counters', 'FileSystemCounters'}


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(4 << 20), b''):
            h.update(block)
    return h.hexdigest()


def build_jar(script, cwd, dest):
    subprocess.run([str(script)], cwd=str(cwd), env=ENV, check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    shutil.copy2(cwd / 'jobs/target/behavior-jobs.jar', dest)


def build_old_jar(work):
    src = work / 'head-src'
    subprocess.run(['git', 'worktree', 'add', '--detach', str(src), 'HEAD'],
                   cwd=str(ROOT), env=ENV, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        # build-jobs.sh 的 dev-env.sh 从 worktree 相对定位工具链；复用本仓库已安装的工具。
        (src / 'var').symlink_to(ROOT / 'var', target_is_directory=True)
        (src / '.venv').symlink_to(ROOT / '.venv', target_is_directory=True)
        build_jar(src / 'scripts/build-jobs.sh', src, work / 'old.jar')
    finally:
        subprocess.run(['git', 'worktree', 'remove', '--force', str(src)],
                       cwd=str(ROOT), env=ENV, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


def generate_synthetic(path, rows=200000):
    rng = random.Random(20261120)
    users = [str(i) for i in range(1, 4000)]
    items = [str(i) for i in range(1, 2500)]
    categories = [str(i) for i in range(1, 80)]
    types = ('pv', 'buy', 'cart', 'fav')
    duplicates = []
    lines = []
    for i in range(rows):
        if i and i % 31 == 0:
            lines.append('')                                    # 空行 -> BAD_FIELD_COUNT
        elif i and i % 97 == 0:
            lines.append(f'{i},2,3,pv,1,extra')                 # BAD_FIELD_COUNT
        elif i and i % 211 == 0:
            lines.append(f'not_a_number,2,3,pv,{BASE_TS}')      # BAD_ID
        elif i and i % 307 == 0:
            lines.append(f'{i},2,3,oops,{BASE_TS}')             # BAD_BEHAVIOR
        elif i and i % 401 == 0:
            lines.append(f'{i},2,3,pv,99999999999999999999999') # BAD_TIMESTAMP
        elif i == 17:
            lines.append('1,2,3,pv, 1511575200')                # BAD_TIMESTAMP (空格)
        else:
            day = rng.randrange(-2, 12)                         # 含窗口外日期
            ts = BASE_TS + day * 86400 + rng.randrange(86400)
            u = rng.choice(users)
            if rng.random() < .002:
                u = '0' * rng.randrange(1, 5) + u              # 保留前导零
            if rng.random() < .0003:
                token = '+' + str(ts)                           # 保留 '+' token
            elif rng.random() < .0003:
                token = '000' + str(ts)                         # 保留前导零 token
            else:
                token = str(ts)
            line = f'{u},{rng.choice(items)},{rng.choice(categories)},{rng.choice(types)},{token}'
            if len(duplicates) < 70 and rng.random() < .00035:
                duplicates.append(line)
            if len(duplicates) > 25 and rng.random() < .00035:
                lines.append(rng.choice(duplicates))            # 完全相同的五字段重复行
            else:
                lines.append(line)
    lines.append('1,2,3,pv,-2147483648')                         # 越界但可转换 -> outside
    lines.append('1,2,3,pv,2147483647')                          # 越界但可转换 -> outside
    lines.extend(duplicates[:40])                                # 确保有足够重复组
    payload = []
    for i, line in enumerate(lines):
        if i == 5:
            payload.append(b'\xff\xfe,2,3,pv,1511575200')        # BAD_ENCODING
        elif i % 503 == 0:
            payload.append((line + '\r').encode())               # CRLF
        else:
            payload.append(line.encode())
    # 最后一行无换行，且全文只有此处没有终结符。
    path.write_bytes(b'\n'.join(payload))


def run_stage(stage, jar, work, dataset, inputs, output, tag, log):
    source = work / 'inputs' / dataset / 'source.csv'
    args = ['hadoop', 'jar', str(jar), 'cn.behavior.Jobs',
            '-Dmapreduce.framework.name=local', '-Dfs.defaultFS=file:///',
            f'-Dhadoop.tmp.dir={work / "hadoop-tmp"}',
            f'-Dmapreduce.jobtracker.staging.root.dir={work / "hadoop-tmp" / "staging"}',
            '-Dmapreduce.task.io.sort.mb=16',
            f'-Dbehavior.run.id=eq-{tag}-{dataset}-{stage}',
            '-Dbehavior.reducers=2',
            f'-Dbehavior.source.sha256={sha256(source)}',
            f'-Dbehavior.dataset.start={START_DATE}', f'-Dbehavior.dataset.days={DAYS}',
            stage, ','.join(inputs), str(output)]
    started = time.monotonic()
    with log.open('wb') as stream:
        result = subprocess.run(args, cwd=str(ROOT), env=ENV, stdout=stream,
                                stderr=subprocess.STDOUT)
    elapsed = time.monotonic() - started
    if result.returncode:
        raise RuntimeError(f'{stage} ({tag}/{dataset}) 失败，日志见 {log}:\n' + '\n'.join(
            log.read_text(errors='replace').splitlines()[-25:]))
    return json.loads((output / '_metrics.json').read_text()), elapsed


def dump_sequence(path, jar):
    args = ['hadoop', 'jar', str(jar), 'cn.behavior.Files',
            '-Dfs.defaultFS=file:///',
            f'-Dbehavior.dataset.start={START_DATE}', f'-Dbehavior.dataset.days={DAYS}',
            'dump', str(path)]
    result = subprocess.run(args, cwd=str(ROOT), env=ENV, input=str(path) + '\n',
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f'Files dump 失败: {path}\n{result.stderr[-1000:]}')
    return result.stdout.splitlines()


def collect(output, jar):
    """按逻辑目录归组（忽略 part 文件名/数量），记录排序后比较。"""
    grouped = {}
    for path in sorted(output.rglob('*')):
        if not path.is_file() or path.name.startswith('.') or path.name in ('_SUCCESS', '_metrics.json'):
            continue
        with path.open('rb') as stream:
            header = stream.read(3)
        if header == b'SEQ':
            records = dump_sequence(path, jar)
            logical = str(path.parent.relative_to(output))
        elif 'rejected' in path.parts:
            raw = path.read_bytes()
            if path.name.endswith('.deflate'):
                raw = zlib.decompress(raw)  # Hadoop DefaultCodec 带 zlib 头
            records = raw.decode('utf-8', 'replace').splitlines()
            logical = 'rejected'
        else:
            continue
        grouped.setdefault(logical, []).extend(records)
    return {key: sorted(value) for key, value in grouped.items()}


def business_counters(metrics):
    return {name: values for name, values in metrics['counters'].items()
            if not name.startswith('org.apache.hadoop.') and name not in FRAMEWORK_GROUPS}


def profile_from_lines(lines):
    result = {}
    for line in lines:
        key, value = line.split('\t')
        result[key] = result.get(key, 0) + int(value)
    return result


def compare(label, old, new):
    if old != new:
        raise AssertionError(f'{label} 不一致: old={old!r} new={new!r}')
    print(f'  {label}: 完全一致')


def verify_dataset(work, dataset, csv, old_jar, new_jar):
    print(f'\n=== 数据集 {dataset} ({sum(1 for _ in csv.open("rb"))} 行) ===')
    results = {}
    for tag, jar in (('old', old_jar), ('new', new_jar)):
        base = work / 'runs' / dataset / tag
        base.mkdir(parents=True, exist_ok=True)
        logs = base / 'logs'
        logs.mkdir(exist_ok=True)
        j01 = base / 'j01'
        metrics1, t1 = run_stage('J01', jar, work, dataset,
                                 [f'file://{(work / "inputs" / dataset).resolve()}'],
                                 j01, tag, logs / 'j01.log')
        facts = sorted(p for p in (j01 / 'facts').rglob('part-*') if not p.name.startswith('.'))
        j02 = base / 'j02'
        metrics2, t2 = run_stage('J02', jar, work, dataset,
                                 [str(p) for p in facts], j02, tag, logs / 'j02.log')
        results[tag] = dict(metrics1=metrics1, metrics2=metrics2, t1=t1, t2=t2,
                            out1=collect(j01, jar), out2=collect(j02, jar))
    old, new = results['old'], results['new']

    for stage, key in (('J01', 'metrics1'), ('J02', 'metrics2')):
        print(f'  {stage} counters: old={business_counters(old[key])}')
        compare(f'{stage} 业务 counters', business_counters(old[key]), business_counters(new[key]))

    for stage, key in (('J01', 'out1'), ('J02', 'out2')):
        counts = {k: len(v) for k, v in old[key].items()}
        print(f'  {stage} 输出记录数（按逻辑目录）: {counts}')
        compare(f'{stage} 按目录排序后的记录', old[key], new[key])

    summary_old = old['out2'].get('.', [])
    summary_new = new['out2'].get('.', [])
    compare('J02 汇总行', sorted(summary_old), sorted(summary_new))
    parsed_old, parsed_new = profile_from_lines(summary_old), profile_from_lines(summary_new)
    compare('Python 侧 profile 求和', parsed_old, parsed_new)
    counters_profile = business_counters(new['metrics2']).get('PROFILE', {})
    compare('PROFILE counters 与汇总一致', {k: parsed_old[k] for k in counters_profile}, counters_profile)
    print(f'  profile(节选): ' + ', '.join(f'{k}={v}' for k, v in sorted(parsed_old.items())))

    for stage, tk in (('J01', 't1'), ('J02', 't2')):
        print(f'  耗时 {stage}: old={old[tk]:.1f}s new={new[tk]:.1f}s 比值={old[tk] / new[tk]:.2f}x')
    print(f'  耗时 合计: old={old["t1"] + old["t2"]:.1f}s new={new["t1"] + new["t2"]:.1f}s '
          f'比值={(old["t1"] + old["t2"]) / (new["t1"] + new["t2"]):.2f}x')
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep', action='store_true', help='保留生成的 jar/输入/日志/输出')
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix='behavior-equivalence-'))
    print(f'工作目录: {work}')
    try:
        build_old_jar(work)
        build_jar(ROOT / 'scripts/build-jobs.sh', ROOT, work / 'new.jar')
        print('旧/新 jar 构建完成')
        inputs = work / 'inputs'
        fixture = ROOT / 'var/fixtures/sample-a.csv'
        for name, source in (('sample-a', fixture), ('synthetic', None)):
            target = inputs / name / 'source.csv'
            target.parent.mkdir(parents=True, exist_ok=True)
            if source:
                shutil.copyfile(source, target)
            else:
                generate_synthetic(target)
        for name in ('sample-a', 'synthetic'):
            verify_dataset(work, name, inputs / name / 'source.csv', work / 'old.jar', work / 'new.jar')
        print('\n全部等价检查通过。框架 counters（任务数/ shuffle 字节等）因 map-only 与紧凑 D 键而不同，未计入业务 counters。')
    finally:
        if args.keep:
            print(f'已保留工作目录: {work}')
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())
