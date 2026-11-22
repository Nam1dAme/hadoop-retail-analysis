import os
from pathlib import Path
import sys
from orchestration.common import ROOT, load

config_path = Path(os.environ.get('BEHAVIOR_CONFIG') or (ROOT / 'config/development.toml'))
config = load(config_path)
api_host = config['api']['host']
if api_host in ('0.0.0.0', '::', ''):
    api_host = '127.0.0.1'
env = dict(os.environ, BEHAVIOR_API_URL=f'http://{api_host}:{config["api"]["port"]}/api/v1')
os.execvpe(
    sys.executable,
    [
        sys.executable,
        '-m',
        'streamlit',
        'run',
        str(Path(__file__).parent / 'main.py'),
        '--server.address',
        config['ui']['host'],
        '--server.port',
        str(config['ui']['port']),
        '--server.headless',
        'true',
        '--browser.gatherUsageStats',
        'false',
    ],
    env,
)
