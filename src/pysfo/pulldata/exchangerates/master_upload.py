#%%

from pathlib import Path
import pandas as pd
from typing import Union

#%%========== important parameters ==========%%#

yf_er_downloaddate = "2026-02-20"
yf_er_period = "max"

#%%========== helper functions ==========%%#

def _clean_fred_ER_file(er_fred_code):

    import pandas as pd
    import pysfo.pulldata as pysfo_pull

    filepath = pysfo_pull.get_data_path() / f"exchangerates_fred/{er_fred_code}.csv"

    df = pd.read_csv(filepath, index_col=0, parse_dates=True).reset_index()
    df.columns = ["date", "er"]

    return df


def _get_yfinance_exchangerates(
    ccy_group,
    interval
):

    import pandas as pd
    from pysfo.basic import load_parquet
    import pysfo.pulldata as pysfo_pull
    import numpy as np
    from pysfo.pulldata.exchangerates.yfDownload.download_params import yf_er_ccypair_fetch_root_name
    from pysfo.pulldata.exchangerates.utils import add_fetch_stamps_to_filename
    
    # from pysfo.basic import *
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    # # ########
    # ccy_group = "DM_G10"
    # interval = "1d"
    # # ########
    
    yf_er = pysfo_pull.get_data_path() / "exchangerates_yfinance"

    _file = add_fetch_stamps_to_filename(
        yf_er_ccypair_fetch_root_name, 
        downloaddate=yf_er_downloaddate,
        period=yf_er_period,
        interval=interval,
    ).format(ccy_group = ccy_group)

    _file = yf_er / (_file + ".parquet")
    df = load_parquet(_file)
    
    # rename and reset index

    df.reset_index(inplace = True)
    df.columns = df.columns.str.lower()
    
    # fix formats

    _numeric_vars = ["open", "high", "low", "close"]
    _date_vars = ["datetime", "date"]
    
    for var in _numeric_vars:
        df[var] = pd.to_numeric(df[var], errors = "coerce")

    for var in _date_vars:
        try:
            df[var] = pd.to_datetime(df[var], errors = "coerce")
        except:
            pass

    # all currency pairs as foreign currency / USD

    fx_per_dollar = [
        "USDCAD=X", 
        "USDCHF=X",
        "USDJPY=X",
        "USDNOK=X", 
        "USDSEK=X", 
    ]

    dollar_per_fx = [
        "AUDUSD=X", 
        "EURUSD=X", 
        "GBPUSD=X", 
        "NZDUSD=X"
    ]

    df["ccy"] = df["ccy_pair"].apply(
        lambda x: x.replace("USD", "").replace("=X", "")
    )

    price_cols = ["open", "high", "low", "close"]

    mask = df["ccy_pair"].isin(dollar_per_fx)

    for col in price_cols:
        df.loc[mask, col] = 1 / df.loc[mask, col]

    # keep final columns and return

    non_assigned = (
        ~(df["ccy_pair"].isin(fx_per_dollar))
        & ~(df["ccy_pair"].isin(dollar_per_fx))
    )

    df["quote_type"] = np.where(non_assigned, "not_checked", "fx_per_dollar")
    df["ccy_pair"] = df["ccy_pair"].apply(
        lambda x: x.replace("=X", "")
    )

    # keepvars and formats

    df["ccy_group"] = ccy_group
    df = df.rename(columns = {"close" : "er"})
    keep_vars = ["date", "er", "ccy_pair", "ccy", "ccy_group", "quote_type"]
    df = df[keep_vars]
    df["date"] = df["date"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["date"] = df["date"].dt.normalize()

    # return

    return df

def _get_fred_exchangerates(
    interval
):

    from pysfo.basic import load_parquet
    import pysfo.pulldata as pysfo_pull
    import numpy as np
    from pysfo.pulldata.exchangerates.params.ccy_pairs import ccy_pairs
    from pysfo.pulldata.exchangerates.utils import add_fetch_stamps_to_filename
    
    # from pysfo.basic import *
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    # # ########
    # ccy_group = "DM_G10"
    # downloaddate = "2026-02-20"
    # period = "max"
    # interval = "1d"
    # # ########

    # fred only available for daily data
    if interval == "1d":
        pass
    else:
        return None

    df = pd.concat([
        (
            _clean_fred_ER_file(ccy_info_dict["fred"])
            .assign(
                ccy_group = ccy_group,
                ccy_pair = ccy_iso3
            )
        )
        for ccy_group, ccy_dict in ccy_pairs.items()
        for ccy_iso3, ccy_info_dict in ccy_dict.items()
        if (
            ccy_info_dict["fred"] is not None
        )

    ], axis = 0)
    
    # fix formats

    _numeric_vars = ["er"]
    _date_vars = ["date"]
    
    for var in _numeric_vars:
        df[var] = pd.to_numeric(df[var], errors = "coerce")

    for var in _date_vars:
        try:
            df[var] = pd.to_datetime(df[var], errors = "coerce")
        except:
            pass

    # all currency pairs as foreign currency / USD

    fx_per_dollar = [
        "USDBRL",
        "USDCAD",
        "USDCHF",
        "USDCNY",
        "USDINR",
        "USDJPY",
        "USDKRW",
        "USDMXN",
        "USDMYR",
        "USDNOK",
        "USDSEK",
        "USDZAR",
    ]

    dollar_per_fx = [
        "AUDUSD",
        "EURUSD",
        "GBPUSD",
        "NZDUSD",
    ]

    df["ccy"] = df["ccy_pair"].apply(
        lambda x: x.replace("USD", "").replace("=X", "")
    )

    price_cols = ["er"]

    mask = df["ccy_pair"].isin(dollar_per_fx)

    for col in price_cols:
        df.loc[mask, col] = 1 / df.loc[mask, col]

    # keep final columns and return
    
    non_assigned = (
        ~(df["ccy_pair"].isin(fx_per_dollar))
        & ~(df["ccy_pair"].isin(dollar_per_fx))
    )

    df["quote_type"] = np.where(non_assigned, "not_checked", "fx_per_dollar")

    # keepvars and formats

    keep_vars = ["date", "er", "ccy_pair", "ccy", "ccy_group", "quote_type"]
    df = df[keep_vars]

    # return

    return df

#%%========== data retriever ==========%%#

def H_get(
    interval : str,
    ccy_group : Union[str, None] = None, 
) -> Union[pd.DataFrame, None]:

    import pandas as pd
    from pysfo.basic import load_parquet
    import pysfo.pulldata as pysfo_pull
    from pysfo.pulldata.exchangerates.params.ccy_pairs import ccy_pairs
    from pysfo.pulldata.exchangerates.yfDownload.download_params import yf_er_ccypair_fetch_root_name
    from pysfo.pulldata.exchangerates.utils import add_fetch_stamps_to_filename
    
    # from pysfo.basic import *
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    # # ########
    # ccy_group = "DM_G10"
    # downloaddate = "2026-02-20"
    # period = "max"
    # interval = "1d"
    # # ########

    # import all available currencies for yfinance and fred

    yf_er_data = pd.concat([
        _get_yfinance_exchangerates(
            ccy_group,
            interval
        )
        for ccy_group in ccy_pairs.keys()
    ], axis = 0)

    fred_er_data = _get_fred_exchangerates(interval)

    er_data = {
        "yfinance" : {
            ccy_pair : data
            for ccy_pair, data in yf_er_data.groupby("ccy_pair")
        },
        "fred" : {
            ccy_pair : data
            for ccy_pair, data in fred_er_data.groupby("ccy_pair")
        } if fred_er_data is not None else None
    }

    # keep data by best data source available
    
    er_selection = [
        (ccy_iso3, "fred" if ccy_info_dict["fred"] else "yfinance")
        for _, ccy_dict in ccy_pairs.items()
        for ccy_iso3, ccy_info_dict in ccy_dict.items()
    ]

    er_data = { ccy_pair : er_data[source].get(ccy_pair)
        for (ccy_pair, source) in er_selection
    }

    not_available = [{ccy_iso3 : data} for ccy_iso3, data in er_data.items() if data is None]
    
    if len(not_available) > 0:
        print(f"Data not available for {not_available}")

    yfinance_pairs = [ccy_iso3 for ccy_iso3, source in er_selection if source == "yfinance"]
    fred_pairs = [ccy_iso3 for ccy_iso3, source in er_selection if source == "fred"]

    if len(yfinance_pairs) > 0 :

        _series = ", ".join(yfinance_pairs)
        _msg = (
            "Series obtained from yfinance:\n" \
            f"\t{_series}"
        )
        print(_msg)

    if len(fred_pairs) > 0 :
        _series = ", ".join(fred_pairs)
        _msg = (
            "Series obtained from fred:\n" \
            f"\t{_series}"
        )
        print(_msg)

    # consolidate

    er_data = pd.concat([
        data 
        for data in er_data.values() if data is not None
    ], axis = 0).reset_index(drop = True)

    # filter by required ccy group

    if ccy_group:

        _ccy_group_list = [ccy_group] if isinstance(ccy_group, str) else ccy_group
        mask = er_data["ccy_group"].isin(_ccy_group_list)
        er_data = er_data[mask]

    # return

    return er_data
