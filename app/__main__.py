import os
from pathlib import Path
import sys
from orchestration.common import ROOT,load

config=load(ROOT/'config/development.toml')
env=dict(os.environ,BEHAVIOR_API_URL=f'http://{config["api"]["host"]}:{config["api"]["port"]}/api/v1')
os.execvpe(sys.executable,[sys.executable,'-m','streamlit','run',str(Path(__file__).parent/'main.py'),
                                 '--server.address',config['ui']['host'],'--server.port',str(config['ui']['port']),
                                 '--server.headless','true','--browser.gatherUsageStats','false'],env)
