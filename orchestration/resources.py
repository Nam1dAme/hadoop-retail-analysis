"""WSL host-volume free space is distinct from virtual ext4 free space."""
import json
from pathlib import Path
import re
import subprocess
import threading
import time
from .common import ROOT

_lock=threading.Lock()
_cached=None

def host_free_bytes():
    global _cached
    with _lock:
        now_m=time.monotonic()
        if _cached is not None and now_m-_cached[0]<30:return _cached[1]
        executable=Path('/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')
        result=None
        try:
            if executable.is_file():
                source=json.loads((ROOT/'config/environment.lock.json').read_text())['host_storage'][0]['host_volume']
                letter=source[0].upper()
                if re.fullmatch('[A-Z]',letter):
                    command="[System.IO.DriveInfo]::new('"+letter+":\\').AvailableFreeSpace"
                    p=subprocess.run([str(executable),'-NoProfile','-NonInteractive','-Command',command],capture_output=True,text=True,timeout=5)
                    if p.returncode==0:result=int(p.stdout.strip())
        except (OSError,ValueError,KeyError,IndexError,subprocess.TimeoutExpired):pass
        _cached=(now_m,result)
        return result
