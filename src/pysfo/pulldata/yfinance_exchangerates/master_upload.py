#%%========== data retriever ==========%%#

def get(ccy_group, **kwargs):

    import pandas as pd
    from pysfo.basic import load_parquet
    import pysfo.pulldata as pysfo_pull
    from pysfo.pulldata.yfinance_exchangerates.yfDownload.exchangerates_params import yf_er_ccypair_fetch_root_name
    from pysfo.pulldata.yfinance_exchangerates.yfDownload.utils import add_fetch_stamps_to_filename
    
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    # ########
    # ccy_group = "DM_G10"
    # kwargs = {
    #     "period" : "max",
    #     "interval" : "1h"
    # }
    #########
    
    if_er = pysfo_pull.get_data_path() / "yfinance_exchangerates"

    _file = add_fetch_stamps_to_filename(yf_er_ccypair_fetch_root_name, **kwargs).format(ccy_group = ccy_group)

    try :
        _file = if_er / (_file + ".parquet")
        df = load_parquet(_file)
    except FileNotFoundError:

        _msg = (
            f"yfinance exchangerates file for {ccy_group} not found.\n" \
            "Try running fetch_and_save_yfinance_er_by_ccy_group() first."
        )
        raise FileNotFoundError(_msg)

    # rename and reset index

    df.reset_index(inplace = True)
    df.columns = df.columns.str.lower()
    
    # fix formats

    _numeric_vars = ["open", "high", "low", "close"]
    _date_vars = ["datetime"]
    
    for var in _numeric_vars:
        df[var] = pd.to_numeric(df[var], errors = "coerce")

    for var in _date_vars:
        df[var] = pd.to_datetime(df[var], errors = "coerce")

    # return

    return df

__all__ = [
    "get"
]