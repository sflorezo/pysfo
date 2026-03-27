#%% 

from pathlib import Path
from typing import Union
import pandas as pd

# from pysfo.basic import *
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_rates/")

#%% ========== old

def H_process_rfr(
        benchmark_rates_dir,
        cty_group : Union[str, list, None] = None,
        ccy_iso3 : Union[str, list, None] = None
):
    
    from pysfo.pulldata.interestRates.benchmark_rates.process.lseg_downloaded import get_lseg_rfr
    from pysfo.pulldata.interestRates.benchmark_rates.process.manually_downloaded import get_manually_downloaded_rfr

    # get data from different sources

    df_lseg = get_lseg_rfr(benchmark_rates_dir)
    df_manual = get_manually_downloaded_rfr(benchmark_rates_dir)

    # consolidate and apply country filters

    df_appended = pd.concat([df_lseg, df_manual], axis = 0)
    del df_lseg, df_manual

    if cty_group:
        cty_group_list = [cty_group] if isinstance(cty_group, str) else cty_group
        invalid_groups = set(cty_group_list) - set(df_appended["group"].unique())
        if invalid_groups:
            raise ValueError(f"cty_group not found in data: {invalid_groups}")
        df_appended = df_appended[df_appended["group"].isin(cty_group_list)]

    if ccy_iso3:
        ccy_iso3_list = [ccy_iso3] if isinstance(ccy_iso3, str) else ccy_iso3
        invalid_ccys = set(ccy_iso3_list) - set(df_appended["ccy"].unique())
        if invalid_ccys:
            raise ValueError(f"ccy_iso3 not found in data{' for the selected cty_group' if cty_group else ''}: {invalid_ccys}")
        df_appended = df_appended[df_appended["ccy"].isin(ccy_iso3_list)]

    # Clean: I am now doing this in each individual file, which might be better suited as I am collecting piece by piece.
    # # fix formats

    # _numeric_vars = ["rate"]
    
    # for var in _numeric_vars:
    #     df_appended[var] = pd.to_numeric(df_appended[var], errors = "coerce")

    # # fix units

    # in_percentages = [
    #     {"ccy": "USD", "benchmark": "SOFR"},
    #     {"ccy": "USD", "benchmark": "LIBOR"},
    #     {"ccy": "EUR", "benchmark": "ESTR"},
    #     {"ccy": "EUR", "benchmark": "EONIA"},
    #     {"ccy": "GBP", "benchmark": "SONIA"},
    #     {"ccy": "JPY", "benchmark": "TONAR"},
    #     {"ccy": "CHF", "benchmark": "SARON"},
    #     {"ccy": "CAD", "benchmark": "CORRA"},
    #     {"ccy": "SEK", "benchmark": "SWESTR"},
    #     {"ccy": "AUD", "benchmark": "AONIA"},
    #     {"ccy": "NZD", "benchmark": "NZONIA"},
    #     {"ccy": "NOK", "benchmark": "NOWA"},
    # ]
    
    # for correct in in_percentages:
    #     mask = (df_appended["ccy"] == correct["ccy"]) & (df_appended["benchmark"] == correct["benchmark"])
    #     df_appended.loc[mask, "rate"] = df_appended.loc[mask, "rate"] / 100
    
    return df_appended

__all__ = [
    "H_process_rfr"
]
