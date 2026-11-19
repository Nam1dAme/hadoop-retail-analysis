#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/dev-env.sh"
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
cd "$BEHAVIOR_ROOT"
PYTHON_BIN="$BEHAVIOR_ROOT/.venv/bin/python"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi
case "${1:-status}" in
  init)
    "$PYTHON_BIN" - <<'PY'
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
    for service in namenode datanode resourcemanager nodemanager historyserver; do
      case "$service" in
        namenode|datanode) tool=hdfs ;;
        historyserver) tool=mapred ;;
        *) tool=yarn ;;
      esac
      # Hadoop 3.4.2 can return zero after printing "is stopped"; inspect its status text.
      service_status=$("$tool" --daemon status "$service" 2>&1 || true)
      if [[ "$service_status" == "$service is running as process "* ]]; then
        echo "$service 已运行"
      else
        setsid -w "$tool" --daemon start "$service"
      fi
    done
    timeout 120 hdfs dfsadmin -safemode wait
    "$PYTHON_BIN" - <<'PYREADY'
import json,sys,time,urllib.request
from pathlib import Path
sys.path.insert(0,str(Path('.').resolve()))
from scripts._common import history_base_url,load_dev_config,yarn_base_url
cfg=load_dev_config()
yarn_url=yarn_base_url(cfg)
history_url=history_base_url(cfg)
for _ in range(60):
 try:
  with urllib.request.urlopen(yarn_url+'/ws/v1/cluster/nodes',timeout=2) as r:
   nodes=json.load(r).get('nodes') or {}
  if any(n['state']=='RUNNING' for n in nodes.get('node',[])):
   print('HDFS 可写、YARN 节点已就绪');break
 except (OSError,ValueError):pass
 time.sleep(2)
else:raise SystemExit('YARN节点未在120秒内就绪')
for _ in range(60):
 try:
  with urllib.request.urlopen(history_url+'/ws/v1/history/info',timeout=2) as r:
   assert json.load(r)['historyInfo']
  print('MapReduce HistoryServer 已就绪');break
 except (OSError,ValueError,KeyError,AssertionError):pass
 time.sleep(1)
else:raise SystemExit('HistoryServer未在60秒内就绪')
PYREADY
    ;;
  stop)
    mapred --daemon stop historyserver
    yarn --daemon stop nodemanager
    yarn --daemon stop resourcemanager
    hdfs --daemon stop datanode
    hdfs --daemon stop namenode
    ;;
  status)
    hdfs dfsadmin -report
    yarn node -list
    mapred --daemon status historyserver
    ;;
  *) echo '用法: hadoop-service.sh init|start|stop|status'; exit 2 ;;
esac
