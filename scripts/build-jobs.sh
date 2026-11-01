#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/dev-env.sh"
cd "$BEHAVIOR_ROOT"
python - <<'PY'
from pathlib import Path
import os,subprocess,tempfile,zipfile
root=Path.cwd();target=root/'jobs/target';target.mkdir(exist_ok=True)
classpath=subprocess.check_output(['hadoop','classpath','--glob'],text=True).strip()
with tempfile.TemporaryDirectory(dir=target) as work:
    sources=sorted((root/'jobs/src/main/java/cn/behavior').glob('*.java'))
    subprocess.run(['javac','--release','11','-cp',classpath,'-d',work,*map(str,sources)],check=True)
    tmp=target/'behavior-jobs.tmp.jar'
    with zipfile.ZipFile(tmp,'w',compression=zipfile.ZIP_STORED) as z:
        entries={'META-INF/MANIFEST.MF':b'Manifest-Version: 1.0\n\n'}
        entries.update({str(p.relative_to(work)):p.read_bytes() for p in sorted(Path(work).rglob('*.class'))})
        for name,data in sorted(entries.items()):
            entry=zipfile.ZipInfo(name,date_time=(1980,1,1,0,0,0));entry.external_attr=0o644<<16;z.writestr(entry,data)
    os.replace(tmp,target/'behavior-jobs.jar')
print(target/'behavior-jobs.jar')
PY
