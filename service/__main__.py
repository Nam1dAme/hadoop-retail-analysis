import uvicorn
from orchestration.common import ROOT,load
from .api import create_app

config=load(ROOT/'config/development.toml')
uvicorn.run(create_app(config),host=config['api']['host'],port=config['api']['port'],access_log=False)
