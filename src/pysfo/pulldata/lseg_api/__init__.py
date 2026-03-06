#%%

import os
from pathlib import Path
import lseg.data as ld   # or refinitiv.data as rd
from pysfo.pulldata.lseg_api.configs.configs import build_configs
import inspect

def _get_caller_dir():
    this_file = Path(__file__).resolve()
    for frame in inspect.stack():
        path = Path(frame.filename).resolve()
        if (
            path != this_file
            and "site-packages" not in str(path)
            and "interactiveshell" not in str(path).lower()
        ):
            return path.parent
    return Path.cwd()

class lsgeData:

    def __init__(
            self, 
            app_key, 
            username, 
            password
    ):

        
        
        caller_path_dir = _get_caller_dir()
        configs_path = caller_path_dir / "configs"

        _configs_file_name_str = str(configs_path / "lseg-data.config")
        print(f"creating LSEG API configs file in \n{_configs_file_name_str}")
        
        configs_path.mkdir(exist_ok=True)
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
        del app_key, username, password

        # OPEN SESSION HERE
        self._ld = ld
        self._ld.open_session(name = "platform.ldp")

    def close(self):
        self._ld.close_session()

    def __repr__(self):
        return "lsegData(session=platform.ldp)"
    
        


