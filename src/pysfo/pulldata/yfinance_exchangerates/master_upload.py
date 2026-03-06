#%%========== data retriever ==========%%#

def H_get(ccy_group, 
        downloaddate,
        period,
        interval):

    import pandas as pd
    from pysfo.basic import load_parquet
    import pysfo.pulldata as pysfo_pull
    from pysfo.pulldata.yfinance_exchangerates.yfDownload.download_params import yf_er_ccypair_fetch_root_name
    from pysfo.pulldata.yfinance_exchangerates.utils import add_fetch_stamps_to_filename
    
    # from pysfo.basic import *
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    # # ########
    # ccy_group = "DM_G10"
    # downloaddate = "2026-02-20"
    # period = "max"
    # interval = "1d"
    # # ########
    
    if_er = pysfo_pull.get_data_path() / "yfinance_exchangerates"

    _file = add_fetch_stamps_to_filename(
        yf_er_ccypair_fetch_root_name, 
        downloaddate=downloaddate,
        period=period,
        interval=interval,
    ).format(ccy_group = ccy_group)

    _file = if_er / (_file + ".parquet")
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
    
    df["quote_type"] = "fx_per_dollar"
    df.drop(columns = "ccy_pair", inplace = True)

    # return

    return df
