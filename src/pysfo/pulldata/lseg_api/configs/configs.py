#%%

from pathlib import Path
import json

def build_configs(app_key : str, username : str, password : str, configs_path: Path):

    data_config = {
        "sessions": {
            "default": "platform.ldp",
            "platform": {
                "ldp": {
                    "signon_control": True,
                    "app-key": app_key,
                    "username": username,
                    "password": password
                },
            },
            "desktop": {
                "workspace": {
                    "app-key": app_key
                }
            }
        }
    }

    config_path = configs_path / "lseg-data.config.json"

    with config_path.open("w", encoding="utf-8") as f:
        json.dump(data_config, f, indent=4)
    