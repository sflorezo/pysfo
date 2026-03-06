#%%

from pysfo.pulldata.interest_rates.benchmark_rates.params.overnight_rfr import overnight_rfr
from pathlib import Path
import pandas as pd
from typing import cast, Union
from pprint import pprint


#%% ========== helper files ========== %%#

def clean_AUD(file_path_args):

    df = pd.read_excel(file_path_args[0], sheet_name = "Data", skiprows = 1)
    df.columns = df.columns.str.lower()

    df = df[["title", "interbank overnight cash rate"]]
    df = df[pd.to_datetime(df["title"], format="mixed", errors="coerce").notna()]
    df["title"] = pd.to_datetime(df["title"], format="mixed").dt.normalize()

    df = df.rename(columns={"title": "date", "interbank overnight cash rate": "rate"})
    df = df.assign(
        group="DM_G10",
        ccy="AUD",
        benchmark="AONIA",
        rate_type = "traded, unsecured",
    )[["date", "group", "ccy", "benchmark", "rate_type", "rate"]]

    return df

def clean_NZD(file_path_args):

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        df_list = []

        for file in file_path_args:

            df = pd.read_excel(file, sheet_name = "Data", skiprows = 1) #  
            df.columns = df.columns.str.lower()

            df = df[["unnamed: 0", "overnight interbank cash rate"]]
            df = df[pd.to_datetime(df["unnamed: 0"], format="mixed", errors="coerce").notna()]
            df["unnamed: 0"] = pd.to_datetime(df["unnamed: 0"], format="mixed").dt.normalize()

            df_list.append(df)

        df = pd.concat(df_list, axis=0)
    
    df = df.rename(columns={"unnamed: 0": "date", "overnight interbank cash rate": "rate"})
    df = df.assign(
        group="DM_G10",
        ccy="NZD",
        benchmark="NZONIA",
        rate_type="traded, unsecured",
    )[["date", "group", "ccy", "benchmark", "rate_type", "rate"]]

    return df

def clean_NOK(file_path_args):

    df = pd.read_csv(file_path_args[0], sep=';', dtype=str)
    df.columns = df.columns.str.lower()

    df = df[df["unit_measure"] == "R"]
    
    df = df[["time_period", "obs_value"]]
    df = df[pd.to_datetime(df["time_period"], format="mixed", errors="coerce").notna()]
    df[""] = pd.to_datetime(df["time_period"], format="mixed").dt.normalize()

    df = df.rename(columns={"time_period": "date", "obs_value": "rate"})
    df = df.assign(
        group="DM_G10",
        ccy="NOK",
        benchmark="NOWA",
        rate_type="traded, unsecured",
    )[["date", "group", "ccy", "benchmark", "rate_type", "rate"]]

    return df

#%% ========== get it ========== %%#

def get_manually_downloaded_overnight_rfr(
    benchmark_rates_dir,
):

    """Get the overnight rates downloaded manually"""

    # get manually downloaded configs and data at once

    df_list = [
        globals()[f"clean_{_ccy_iso3}"](
            [benchmark_rates_dir / file_path for file_path in info["source_params"]["file_names"]]
        )
        for _, cty_dict in overnight_rfr.items()
        for _ccy_iso3, info in cty_dict.items()
        if (
            (info["source"] == "manual") 
            and (info["obtained"] == True)
            and (info["available"] == True)
        )
    ]

    # consolidate

    df = pd.concat(df_list, ignore_index=True)

    # fix formats

    df['date'] = pd.to_datetime(df['date']).dt.normalize()

    return df

# %%
