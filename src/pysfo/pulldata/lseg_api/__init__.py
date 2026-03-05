#%%

import os
from pathlib import Path
import lseg.data as ld   # or refinitiv.data as rd
from pysfo.pulldata.lseg_api.configs.configs import build_configs

class lsge_data:

    def __init__(self, app_key, username, password):

        configs_path = Path(__file__).resolve().parent / "configs"
        os.environ["LD_LIB_CONFIG_PATH"] = str(configs_path)

        missing = [
            name for name, value in {
                "app_key": app_key,
                "username": username,
                "password": password,
            }.items()
            if value is None
        ]
        if missing:
            raise ValueError(f"Missing required argument(s): {', '.join(missing)}")

        build_configs(app_key, username, password, configs_path)

        # OPEN SESSION HERE
        self._ld = ld
        self._ld.open_session(name = "platform.ldp")

    def close(self):
        self._ld.close_session()

    def __repr__(self):
        return "lsge_data(session=platform.ldp)"
    
        


