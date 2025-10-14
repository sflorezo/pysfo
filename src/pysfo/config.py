import os
import sys, io
from contextlib import contextmanager


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
    
@contextmanager
def suppress_print():
    saved_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        yield
    finally:
        sys.stdout = saved_stdout

def general_configs():
    
    import pandas as pd

    pd.set_option("display.max_columns", 100)
    pd.set_option("display.max_rows", 300)