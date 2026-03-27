#%%

from pysfo.pulldata.interestRates.benchmark_rates.params.rfr import rfr_list
from pathlib import Path
import pandas as pd
from typing import cast, Union, Dict
from pprint import pprint
import numpy as np
from pysfo.basic import flatten_list

# from pysfo.basic import *
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_rates")


#%% ========== helper files ========== %%#

def return_file_name_list(ccy, name_simplified, tenor = None):

    # #####
    # ccy = "EUR"
    # name_simplified = "EURIBOR"
    # tenor = "3M"
    # #####

    if tenor is None:
        file_list = flatten_list([
            [
                [
                    [f"{ccy_group}/" + file for file in ir_metadata["source_params"]["file_names"]]
                    for ir_metadata in ccy_ir_list
                    if (
                        (ir_metadata["source"] == "manual")
                        and (ir_metadata["name_simplified"] == name_simplified)
                    )
                ]
                for _ccy, ccy_ir_list in ccy_dicts.items()
                if _ccy == ccy
            ]
            for ccy_group, ccy_dicts in rfr_list.items()
        ])
    else:
        file_list = flatten_list([
            [
                [
                    [f"{ccy_group}/" + file for file in ir_metadata["source_params"]["file_names"]]
                    for ir_metadata in ccy_ir_list
                    if (
                        (ir_metadata["source"] == "manual")
                        and (ir_metadata["name_simplified"] == name_simplified)
                        and (ir_metadata["tenor"] == tenor)
                    )
                ]
                for _ccy, ccy_ir_list in ccy_dicts.items()
                if _ccy == ccy
            ]
            for ccy_group, ccy_dicts in rfr_list.items()
        ])

    return file_list


def clean_manually_downloaded_AUD_AONIA(benchmark_rates_dir, ccy = "AUD", name_simplified = "AONIA", tenor = "ON"):

    file_name_list = [
        benchmark_rates_dir / file 
        for file in
        return_file_name_list(ccy, name_simplified, tenor)
    ]

    df = pd.read_excel(file_name_list[0], sheet_name = "Data", skiprows = 1)
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
        tenor = "on"
    )[["date", "group", "ccy", "benchmark", "tenor", "rate_type", "rate"]]

    df["rate"] = pd.to_numeric(df["rate"], errors = "coerce")
    df["rate"] = df["rate"] / 100

    return df

def clean_manually_downloaded_NOK_NOWA(benchmark_rates_dir, ccy = "NOK", name_simplified = "NOWA", tenor = "ON"):

    file_name_list = [
        benchmark_rates_dir / file 
        for file in
        return_file_name_list(ccy, name_simplified, tenor)
    ]

    df = pd.read_csv(file_name_list[0], sep=';', dtype=str)
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
        tenor = "on"
    )[["date", "group", "ccy", "benchmark", "tenor", "rate_type", "rate"]]

    df["rate"] = pd.to_numeric(df["rate"], errors = "coerce")
    df["rate"] = df["rate"] / 100

    return df

def clean_manually_downloaded_EUR_EONIA(benchmark_rates_dir,ccy = "EUR", name_simplified = "EONIA", tenor = "ON"):

    #####
    # file_path_args = [Path(benchmark_rates_dir) / "DM_G10/DM_G10_EUR_EONIA_FRED.xlsx"]
    #####

    file_name_list = [
        benchmark_rates_dir / file 
        for file in return_file_name_list(ccy, name_simplified, tenor)
     
    ]

    df = pd.read_excel(file_name_list[0], dtype=str, sheet_name="Daily")
    df.columns = df.columns.str.lower()

    df = df[pd.to_datetime(df["observation_date"], format="mixed", errors="coerce").notna()]
    df["observation_date"] = pd.to_datetime(df["observation_date"], format="mixed").dt.normalize()

    df = df.rename(columns={"observation_date": "date", "eoniarate": "rate"})
    df = df.assign(
        group="DM_G10",
        ccy="EUR",
        benchmark="EONIA",
        rate_type="traded, unsecured",
        tenor = "on"
    )[["date", "group", "ccy", "benchmark", "tenor", "rate_type", "rate"]]

    df["rate"] = pd.to_numeric(df["rate"], errors = "coerce")
    df["rate"] = df["rate"] / 100

    return df

def clean_manually_downloaded_NZD_NZONIA(benchmark_rates_dir, ccy = "NZD", name_simplified = "NZONIA", tenor = "ON"):

    file_name_list = [
        benchmark_rates_dir / file 
        for file in return_file_name_list(ccy, name_simplified, tenor)
     
    ]

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        df_list = []

        for file in file_name_list:

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
        tenor = "on"
    )[["date", "group", "ccy", "benchmark", "tenor", "rate_type", "rate"]]

    df["rate"] = pd.to_numeric(df["rate"], errors = "coerce")
    df["rate"] = df["rate"] / 100

    return df

def clean_manually_downloaded_USD_LIBOR_1M_ON(benchmark_rates_dir, ccy = "USD", name_simplified = "USD_LIBOR"):

    #####
    # file_path_args = [Path(benchmark_rates_dir) / "DM_G10/DM_G10_USD_LIBOR_MacroMicro.json"]
    #####

    file_name_list = [
        benchmark_rates_dir / file 
        for file in return_file_name_list(ccy, name_simplified)
     
    ]

    from pysfo.basic import load_json

    usd_libor = load_json(file_name_list[0])

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
            benchmark = "USD_LIBOR",
            rate_type = "traded, unsecured",
        ) 
        for _s, _i in zip(series, info)
    ], axis = 0)


    is_tenor_on = df["series"].str.contains("overnight", case = False)
    is_tenor_1m = df["series"].str.contains("1-Month", case = False)

    df["tenor"] = np.nan
    df["tenor"] = np.where(is_tenor_on, "on", df["tenor"])
    df["tenor"] = np.where(is_tenor_1m, "1m", df["tenor"])
    
    mask = (is_tenor_on | is_tenor_1m)
    df["series"] = df["series"].str.replace("_discontinued", "")
    df = df[mask].copy().reset_index(drop = True)

    df = df[["date", "group", "ccy", "benchmark", "tenor", "rate_type", "rate"]]

    df["rate"] = pd.to_numeric(df["rate"], errors = "coerce")
    df["rate"] = df["rate"] / 100
    
    return df

def clean_manually_downloaded_xibor_USD_EUR_GBP_JPY_3M(benchmark_rates_dir, ccy = "USD", name_simplified = "USD_LIBOR", tenor = "3M"):

    #####
    # ccy = "EUR"
    # name_simplified = "EURIBOR"
    # tenor = "3M"
    #####

    # file name list is the same for 3M USD, EUR, GBP and JPY
    file_name_list = [
        benchmark_rates_dir / file 
        for file in return_file_name_list(ccy, name_simplified, tenor) 
    ] 

    from pysfo.basic import load_json

    df = load_json(file_name_list[0])

    data = df["data"]
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
            rate_type = "quoted, unsecured",
        ) 
        for _s, _i in zip(series, info)
    ], axis = 0)

    is_usd = df["series"].str.findall("^US - 3-Month")
    is_gbp = df["series"].str.findall("^UK - 3-Month")
    is_jpy = df["series"].str.findall("^Japan - 3-Month")
    is_eur = df["series"].str.findall("^Eurozone")

    df["ccy"] = np.nan
    df["ccy"] = np.where(is_usd, "USD", df["ccy"])
    df["ccy"] = np.where(is_gbp, "GBP", df["ccy"])
    df["ccy"] = np.where(is_jpy, "JPY", df["ccy"])
    df["ccy"] = np.where(is_eur, "EUR", df["ccy"])

    df["benchmark"] = np.nan
    df["benchmark"] = np.where(is_eur, "EURIBOR", df["ccy"] + "_LIBOR")

    df["tenor"] = "3m"
    df = df[["date", "group", "ccy", "benchmark", "tenor", "rate_type", "rate"]]

    df["rate"] = pd.to_numeric(df["rate"], errors = "coerce")
    df["rate"] = df["rate"] / 100
    
    return df

#%% ========== get it ========== %%#

def get_manually_downloaded_rfr(
    benchmark_rates_dir,
):
    
    from pysfo.basic import dupli_report

    """Get the overnight rates downloaded manually"""

    # get all functions

    _fns_list = [
        name
        for name in globals() if name.startswith('clean_manually_downloaded')
    ]

    df_list = [
        globals()[name](benchmark_rates_dir)
        for name in _fns_list
    ]
    
    # consolidate

    df = pd.concat(df_list, ignore_index=True)
    df = df.drop_duplicates(subset = ["date", "ccy", "benchmark", "tenor"])

    # check metadata AVAILABLE flags are correct for missing series

    for _, ccy_group_dict in rfr_list.items():
        for ccy, ccy_ir_list in ccy_group_dict.items():
            for ir_metadata in ccy_ir_list:
                
                benchmark = ir_metadata["name_simplified"]
                tenor = ir_metadata["tenor"]
                available = ir_metadata["available"]
                source = ir_metadata["source"]

                if available and source == "manual":
                    
                    mask = (
                        (df["tenor"].str.upper() == tenor.upper())
                        & (df["benchmark"].str.upper() == benchmark.upper())
                        & (df["ccy"].str.upper() == ccy.upper())
                    )
                    
                    assert mask.sum() > 0, f"missing data for {ccy} {benchmark} {tenor}, but available flag in metadata is True."
                
                mask = (df["tenor"] == tenor) & (df["benchmark"] == benchmark) & (df["ccy"] == ccy)
                
    # check All available series are somehow described in metadata

    groups_in_data =[{
            "ccy" : ccy.upper(), 
            "benchmark" : benchmark.upper(),
            "tenor" : tenor.upper()}
        for(ccy, benchmark, tenor), _
        in  df.groupby(["ccy", "benchmark", "tenor"])
    ]
    
    groups_in_metadata = [{
            "ccy" : ccy.upper(),
            "benchmark" : ir_metadata["name_simplified"].upper(),
            "tenor" : ir_metadata["tenor"].upper()
        }
        for ccy_group_dict in rfr_list.values()
        for ccy, ccy_ir_list in ccy_group_dict.items()
        for ir_metadata in ccy_ir_list]
    
    check = [item for item in groups_in_data if item not in groups_in_metadata]

    if check:

        _premsg = []

        for item in check:
            
            _premsg += [f"{item.get("ccy")}, {item.get("benchmark")}, {item.get("tenor")}"]

        _premsg = "\n".join(_premsg)

        _msg =(
            "The following series are available in data, but not in metadata. (please add metadata):\n" \
            f"{_premsg}"
        )

        raise ValueError(_msg)

    # fix formats
    
    df['date'] = pd.to_datetime(df['date']).dt.normalize()

    return df

# %%
