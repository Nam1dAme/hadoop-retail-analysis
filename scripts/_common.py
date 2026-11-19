#!/usr/bin/env python3
"""Shared configuration and DB credential helpers for scripts."""
import configparser
import os
from pathlib import Path
import secrets
import tomllib

ROOT = Path(__file__).resolve().parents[1]
DEFAULTS = {
    'database': {'host': '127.0.0.1', 'port': 3307, 'name': 'behavior_analytics'},
    'hdfs': {'uri': 'hdfs://127.0.0.1:19000', 'root': '/behavior/v1'},
    'api': {'host': '127.0.0.1', 'port': 8010},
    'ui': {'host': '127.0.0.1', 'port': 8510},
    'yarn': {'host': '127.0.0.1', 'port': 18088},
    'history': {'host': '127.0.0.1', 'port': 19888},
}


def load_dev_config(root: Path = ROOT) -> dict:
    """Load config/development.toml with safe fallbacks for every section."""
    path = root / 'config/development.toml'
    raw = tomllib.loads(path.read_text()) if path.is_file() else {}
    merged = {}
    for section, defaults in DEFAULTS.items():
        section_data = dict(defaults)
        if isinstance(raw.get(section), dict):
            section_data.update(raw[section])
        merged[section] = section_data
    for key, value in raw.items():
        if key not in merged:
            merged[key] = value
    return merged


def api_base_url(cfg: dict | None = None) -> str:
    cfg = cfg or load_dev_config()
    api = cfg['api']
    return f"http://{api['host']}:{api['port']}/api/v1"


def ui_base_url(cfg: dict | None = None) -> str:
    cfg = cfg or load_dev_config()
    ui = cfg['ui']
    return f"http://{ui['host']}:{ui['port']}"


def db_endpoint(cfg: dict | None = None) -> tuple[str, int, str]:
    cfg = cfg or load_dev_config()
    db = cfg['database']
    return str(db['host']), int(db['port']), str(db['name'])


def yarn_base_url(cfg: dict | None = None) -> str:
    cfg = cfg or load_dev_config()
    yarn = cfg['yarn']
    return f"http://{yarn['host']}:{yarn['port']}"


def history_base_url(cfg: dict | None = None) -> str:
    cfg = cfg or load_dev_config()
    history = cfg['history']
    return f"http://{history['host']}:{history['port']}"


def ensure_db_secret(path: Path, user: str, database: str, host: str = '127.0.0.1', port: int = 3307) -> str:
    """Create or normalize a 0600 MariaDB client .cnf inside a 0700 secrets directory."""
    secret_dir = path.parent
    secret_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(secret_dir, 0o700)

    password = None
    needs_write = not path.exists()
    if path.exists():
        os.chmod(path, 0o600)
        cfg = configparser.ConfigParser()
        cfg.read(path)
        client = cfg['client'] if cfg.has_section('client') else {}
        password = client.get('password')
        if not password:
            password = secrets.token_hex(24)
            needs_write = True
        if any(client.get(k) != str(v) for k, v in (('user', user), ('host', host), ('port', port), ('database', database))):
            needs_write = True
    else:
        password = secrets.token_hex(24)

    if needs_write:
        text = (
            f"[client]\n"
            f"user={user}\n"
            f"password={password}\n"
            f"host={host}\n"
            f"port={port}\n"
            f"database={database}\n"
        )
        tmp = path.with_suffix(path.suffix + '.tmp')
        if tmp.exists():
            tmp.unlink()
        with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as f:
            f.write(text)
        os.replace(tmp, path)
        os.chmod(path, 0o600)

    return password
