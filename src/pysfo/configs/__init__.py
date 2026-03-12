#%% ========== packages ========== %%#

from pathlib import Path
import os
from pysfo.basic import load_tomli
from pysfo.basic import configure_pandas_display
from pathlib import Path

#%% ========= helper functions

def _expand_paths(path_dict: dict) -> dict:
    path_dict = {
        k : Path(os.path.expandvars(v)) 
        for k, v in path_dict.items()
    }

    return path_dict

#%% ========== upload configs ========== %%#

#--- Unified snapshot for logging or experiment reproducibility

config_path = (
    Path(__file__)
    .resolve()
    .parents[3] / "configs" / "project_params.toml"
)

CONFIGS = load_tomli(config_path)
CONFIGS = {k.upper(): v for k, v in CONFIGS.items()}
CONFIGS = {
    k_0 : {
        k_1 : (Path(val_1) if k_0 == "PATHS" else val_1) for k_1, val_1 in dict_1.items()
    } 
    for k_0, dict_1 in CONFIGS.items()
}

# expand paths

CONFIGS["PATHS"] = _expand_paths(CONFIGS["PATHS"])

#---- Display options for pandas

configure_pandas_display(max_cols=500, max_rows=300)

#---- set api keys

def set_api_keys(api_name = None, api_key = None):
    
    """
    Configure API keys for this package by setting environment variables.

    Parameters
    ----------
    api_name : str
        The API for which to set the key ('openai' or 'fred').
    api_key : str
        The API key to set (optional if already set in environment).
    """
    env_var = None

    if api_name == "openai":
        env_var = "OPENAI_API_KEY"
    elif api_name == "fred":
        env_var = "FRED_API_KEY"
    else:
        raise ValueError("Function has only implemented FRED and OpenAI keys.")

    if os.environ.get(env_var):
        return f"{env_var} key already found in environment. Using user's default."
    elif api_key:
        os.environ[env_var] = api_key
        return f"{env_var} set successfully."
    else:
        raise ValueError(f"No API key provided for {api_name}.")
