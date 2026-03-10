#%%

from pysfo.pulldata.interest_rates.benchmark_rates.params.overnight_rfr import overnight_rfr
from pathlib import Path
import pandas as pd
from typing import cast, Union
from pprint import pprint

# from pysfo.basic import *
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_on_rates")


#%% ========== helper files ========== %%#

def clean_AUD_AONIA(file_path_args):

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

def clean_NOK_NOWA(file_path_args):

    df = pd.read_csv(file_path_args[0], sep=';', dtype=str)
    df.columns = df.columns.str.lower()

    df = df[df["unit_measure"] == "R"]
    
    df = df[["time_period", "obs_value"]]
    df = df[pd.to_datetime(df["time_period"], format="mixed", errors="coerce").notna()]
    df["time_period"] = pd.to_datetime(df["time_period"], format="mixed").dt.normalize()

    df = df.rename(columns={"time_period": "date", "obs_value": "rate"})
    df = df.assign(
        group="DM_G10",
        ccy="NOK",
        benchmark="NOWA",
        rate_type="traded, unsecured",
    )[["date", "group", "ccy", "benchmark", "rate_type", "rate"]]

    return df

def clean_EUR_EONIA(file_path_args):

    #####
    # file_path_args = [Path(benchmark_rates_dir) / "DM_G10_EUR_EONIA_FRED.xlsx"]
    #####

    df = pd.read_excel(file_path_args[0], dtype=str, sheet_name="Daily")
    df.columns = df.columns.str.lower()

    df = df[pd.to_datetime(df["observation_date"], format="mixed", errors="coerce").notna()]
    df["observation_date"] = pd.to_datetime(df["observation_date"], format="mixed").dt.normalize()

    df = df.rename(columns={"observation_date": "date", "eoniarate": "rate"})
    df = df.assign(
        group="DM_G10",
        ccy="EUR",
        benchmark="EONIA",
        rate_type="traded, unsecured",
    )[["date", "group", "ccy", "benchmark", "rate_type", "rate"]]

    return df

def clean_NZD_NZONIA(file_path_args):

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

def clean_USD_LIBOR(file_path_args):

    #####
    # file_path_args = Path(benchmark_rates_dir) / "DM_G10_USD_LIBOR_MacroMicro.json"
    #####

    from pysfo.basic import load_json

    usd_libor = load_json(file_path_args[0])

    data = usd_libor["data"]
    first_key = next(iter(data))
    data = data[first_key]

    info = data["info"]
    series = data["series"]

    info = [cfg["stats"][0]["name_en"] for cfg in info["chart_config"]["seriesConfigs"]]

    df = pd.concat([
        pd.DataFrame(_s)
        .rename(columns = {0 : "date", 1 : "rate"})
        .assign(
            series = _i,
            group = "DM_G10",
            ccy = "USD",
            benchmark = "LIBOR",
            rate_type = "traded, unsecured",
        ) 
        for _s, _i in zip(series, info)
    ], axis = 0)

    mask = df["series"].str.contains("overnight", case = False)
    df["series"] = df["series"].str.replace("_discontinued", "")
    df = df[mask].copy().reset_index(drop = True)

    df = df[["date", "group", "ccy", "benchmark", "rate_type", "rate"]]
    
    return df

#%% ========== get it ========== %%#

def get_manually_downloaded_overnight_rfr(
    benchmark_rates_dir,
):

    """Get the overnight rates downloaded manually"""

    # get manually downloaded configs and data at once

    df_list = [
        globals()["clean_" + ccy_iso3 + "_" + rfr_info["name_simplified"]](
            [benchmark_rates_dir / file_path for file_path in rfr_info["source_params"]["file_names"]]
        )
        for _, ccy_dict in overnight_rfr.items()
        for ccy_iso3, rfr_list in ccy_dict.items()
        for rfr_info in rfr_list
        if rfr_info["source"] == "manual"
        and rfr_info["obtained"] == True
        and rfr_info["available"] == True
    ]

    # consolidate

    df = pd.concat(df_list, ignore_index=True)

    # fix formats
    
    df['date'] = pd.to_datetime(df['date']).dt.normalize()

    return df

# %%
