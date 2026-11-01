#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/dev-env.sh"
cd "$BEHAVIOR_ROOT"
probe_id="$(date -u +%Y%m%dT%H%M%S)-$$"
local_probe="$BEHAVIOR_ROOT/var/probes/$probe_id"
hdfs_probe="/behavior/v1/environment-probes/$probe_id"
mkdir -p "$local_probe/classes"
javac --release 11 -cp "$(hadoop classpath --glob)" -d "$local_probe/classes" scripts/java/BehaviorEnvironmentProbe.java
jar cf "$local_probe/probe.jar" -C "$local_probe/classes" .
cat > "$local_probe/input.csv" <<'DATA'
1,10,100,pv,1511544070
1,10,100,buy,1511544071
2,20,200,pv,1511544072
2,20,200,cart,1511544073
3,30,300,fav,1511544074
DATA
hdfs dfs -mkdir -p "$hdfs_probe/input"
hdfs dfs -put "$local_probe/input.csv" "$hdfs_probe/input/"
hadoop jar "$local_probe/probe.jar" BehaviorEnvironmentProbe "$hdfs_probe/input" "$hdfs_probe/output" 2>&1 | tee "$local_probe/job.log"
hdfs dfs -cat "$hdfs_probe/output/part-r-*" > "$local_probe/result.tsv"
python - "$local_probe" "$hdfs_probe" <<'PY'
import json,re,sys
from pathlib import Path
p=Path(sys.argv[1])
actual=dict(line.split('\t') for line in (p/'result.tsv').read_text().splitlines())
assert actual==dict(buy='1',cart='1',fav='1',pv='2'),actual
ids=re.findall(r'application_\d+_\d+', (p/'job.log').read_text())
assert ids,'Not a real YARN execution'
(p/'evidence.json').write_text(json.dumps(dict(status='PASS',application_id=ids[-1],hdfs_path=sys.argv[2],synthetic=True,counts=actual),indent=2)+'\n')
print((p/'evidence.json').read_text())
PY
