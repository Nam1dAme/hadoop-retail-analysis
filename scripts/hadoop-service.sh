#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/dev-env.sh"
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
cd "$BEHAVIOR_ROOT"
case "${1:-status}" in
  init)
    .venv/bin/python - <<'PY'
from pathlib import Path
import os,subprocess
root=Path(os.environ['BEHAVIOR_ROOT'])
base=root/'var/hdfs'
for part in ['name','data']:
 p=base/part
 if p.is_symlink() or (p.exists() and p.resolve()!=p):raise SystemExit('Refuse redirected HDFS directory')
 if (base/'name/current/VERSION').is_file():
  print('Project NameNode already initialized');break
 if p.exists() and any(p.iterdir()):raise SystemExit('Refuse formatting nonempty storage')
else:
 subprocess.run(['hdfs','namenode','-format','-nonInteractive','-clusterid','CID-behavior-local'],check=True)
PY
    ;;
  start)
    test -f var/hdfs/name/current/VERSION || { echo '请先执行 init'; exit 2; }
    for service in namenode datanode resourcemanager nodemanager; do
      case "$service" in
        namenode|datanode) tool=hdfs ;;
        *) tool=yarn ;;
      esac
      if "$tool" --daemon status "$service" >/dev/null 2>&1; then
        echo "$service 已运行"
      else
        setsid -w "$tool" --daemon start "$service"
      fi
    done
    timeout 120 hdfs dfsadmin -safemode wait
    python - <<'PYREADY'
import json,time,urllib.request
for _ in range(60):
 try:
  with urllib.request.urlopen('http://127.0.0.1:18088/ws/v1/cluster/nodes',timeout=2) as r:
   nodes=json.load(r).get('nodes') or {}
  if any(n['state']=='RUNNING' for n in nodes.get('node',[])):
   print('HDFS 可写、YARN 节点已就绪');break
 except (OSError,ValueError):pass
 time.sleep(2)
else:raise SystemExit('YARN节点未在120秒内就绪')
PYREADY
    ;;
  stop)
    yarn --daemon stop nodemanager
    yarn --daemon stop resourcemanager
    hdfs --daemon stop datanode
    hdfs --daemon stop namenode
    ;;
  status)
    hdfs dfsadmin -report
    yarn node -list
    ;;
  *) echo '用法: hadoop-service.sh init|start|stop|status'; exit 2 ;;
esac
