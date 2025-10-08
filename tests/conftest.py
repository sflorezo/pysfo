#%%========== packages ==========%%#

import os
import pytest
import pysfo.pulldata as pysfo_pull
from setpaths import *
from dotenv import load_dotenv

#%%========== set environment ==========%%#

@pytest.fixture(scope="session", autouse=True)
def setup_env():
    
    home_dotenv = os.path.expanduser("~/.env")
    load_dotenv(home_dotenv)
    pysfo_pull.set_data_path(os.getenv("MASTER_RAW_PATH"))
    pysfo_pull.set_api_keys(api_name="fred", api_key=os.getenv("FRED_API_KEY"))
    pysfo_pull.set_api_keys(api_name="openai", api_key=os.getenv("OPENAI_API_KEY"))

