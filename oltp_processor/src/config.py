import os
from pathlib import Path

import yaml


def load_config(config_path=None):
    if not config_path:
        parent_folder = Path(os.path.abspath(__file__)).parent.parent
        config_path = parent_folder / "config" / "config.yaml"
    print(config_path)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def build_pg_url():
    cfg = load_config()
    pg_cfg = cfg.get("postgres")
    user = pg_cfg.get("user")
    password = pg_cfg.get("password")
    host = pg_cfg.get("host")
    port = pg_cfg.get("port")
    dbname = pg_cfg.get("dbname")
    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{dbname}"
    print(url)
    return url
