
#%% 

import pandas as pd
import dbnomics as db
import os
from joblib import Parallel, delayed
from pysfo.pulldata.dbnomicstools.config import get_filters
from pysfo.basic import silent_call, save_parquet
from pysfo.pulldata import set_data_path, get_data_path
from typing import Dict, Any, cast
import re
from pathlib import Path
import yfinance as yf

# from pysfo.basic import *
# from pysfo.pulldata import set_data_path
# set_data_path("/storage/Dropbox/80_data/raw")
# idx_with_data = [4100:4110]

#%% ========== script-specific params ========== %%#

non_fetched_error_file = "yfinance_er_download_non_fetched_error_file_ccy_group_{ccy_group}.txt"

#%% ========== helper functions ========== %%#

def _fetch_ccy_pair(ccy_pair, **kwargs) -> Dict[str, Any]:

    #######
    # ccy_pair = "EURUSD=X"
    # kwargs = {
    #     "period" : "1d",
    #     "interval" : "1m"
    # }
    # period = "max"
    #######

    not_fetched_error = None
    df = None
    
    try :

        dat = yf.Ticker(ccy_pair)
        df = dat.history(**kwargs)

    except Exception as e:

        not_fetched_error = e
    
    return {
        "ccy_pair" : ccy_pair,
        "fetched_df" : df, 
        "not_fetched_error": not_fetched_error
    }

#%% ========== callable functions ========== %%#

def fetch_and_save_yfinance_er_by_ccy_group(
        root_raw_yfinance_er_path : Path,
        ccy_group : str,
        csv_save_dir : Path, 
        force_fetch = False,
        **kwargs
):
    
    from pysfo.pulldata.exchangerates.params.ccy_pairs import ccy_pairs
    from pysfo.pulldata.exchangerates.yfDownload.download_params import yf_er_ccypair_fetch_root_name
    from pysfo.pulldata.exchangerates.utils import add_fetch_stamps_to_filename
    
    ########
    # root_raw_yfinance_er_path = get_data_path() / "yfinance_exchangerates"
    # ccy_group = "DM_G10"
    # force_fetch = False
    # csv_save_dir = Path("/storage/Dropbox/80_data/raw/yfinance_exchangerates")
    # kwargs = {
    #     "period" : "1d",
    #     "interval" : "1m"
    # }
    #######

    #---- check everything looks good

    _file = add_fetch_stamps_to_filename(filename = yf_er_ccypair_fetch_root_name, **kwargs).format(ccy_group = ccy_group) 

    parquet_save_file = csv_save_dir / (_file + ".parquet")

    if csv_save_dir.is_dir() is False:

        raise ValueError("csv_save_dir must be a directory path. (where the raw data will be stored)")

    if parquet_save_file.exists():
    
        if force_fetch == False:

            _msg = (
                f"File already exists at {parquet_save_file}.\n"
                "Skipping file and continuing with other non-downloaded files (fix force_fetch = True to overwrite)."
            )
            print(_msg)
            return
        
        else:

            _msg = (
                f"File already exists at {parquet_save_file}.\n"
                "Overwriting file (fix force_fetch = False to skip)."
            )

            print(_msg)

    else:
        
        _msg = (
            f"File does not exist for ccy_group = {ccy_group}. Fetching data."
        )
        print(_msg)
        
    print(f"Fetching yfinance Exchange Rate data for ccy_group = {ccy_group}...")

    #---- fetch series main code

    results = Parallel(n_jobs = -1, verbose=10)(
        delayed(_fetch_ccy_pair)(
            batch_dimensions,
            **kwargs
        ) 
        for batch_dimensions in ccy_pairs[ccy_group]
    )

    results = cast(list, results)

    #---- handle errors

    fetch_errors = [
        res["dimensions"]
        for res in results
        if res["not_fetched_error"] is not None
    ]

    if len(fetch_errors) == 0:
        print(f"All series fetched successfully")
    else :
        
        _file = root_raw_yfinance_er_path / Path(str(non_fetched_error_file).format(ccy_group = ccy_group))

        _msg = (
            "Not all series fetched succesfully.\n" \
            f"Storing non-fetched series errors in {_file}"
        )
        print(_msg)
        
        fetch_errors = "\n".join([str(el) for el in fetch_errors])
        _file.write_text(cast(str, fetch_errors))

    #---- handle dataframes

    for i, res in enumerate(results):
        res["fetched_df"] = res["fetched_df"].assign(ccy_pair = res["ccy_pair"])
        results[i] = res

    result_df_list = [res["fetched_df"] for res in results if not res["fetched_df"].empty] 
    
    if len(result_df_list) > 0:
        df = cast(pd.DataFrame, pd.concat(result_df_list, axis = 0))
        save_parquet(df, parquet_save_file, index = True)
        print(f"Data saved in {parquet_save_file}")
        
    else :
        print("No data fetched")
