import argparse
import os
from pathlib import Path
import uvicorn
from orchestration.common import ROOT, load
from .api import create_app


def main():
    parser = argparse.ArgumentParser(description='Run read-only behavior analytics API server')
    parser.add_argument(
        '--config',
        default=os.environ.get('BEHAVIOR_CONFIG') or str(ROOT / 'config/development.toml'),
        help='Path to configuration TOML file',
    )
    args = parser.parse_args()
    config_path = Path(args.config)
    if not config_path.is_absolute() and not config_path.exists():
        config_path = ROOT / config_path
    config = load(config_path)
    uvicorn.run(create_app(config), host=config['api']['host'], port=config['api']['port'], access_log=False)


if __name__ == '__main__':
    main()
