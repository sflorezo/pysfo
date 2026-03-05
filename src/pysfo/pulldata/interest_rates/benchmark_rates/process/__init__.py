
from pathlib import Path
from typing import Union
import pandas as pd

# from pysfo.basic import *
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_on_rates/")

#%% ========== old

def H_process_overnight_rfr(
        benchmark_rates_dir,
        cty_group : Union[str, list, None] = None,
        ccy_iso3 : Union[str, list, None] = None
):
    
    from .lseg_downloaded import get_lseg_overnight_rfr
    from .manually_downloaded import get_manually_downloaded_overnight_rfr

    # get data from different sources

    df_lseg = get_lseg_overnight_rfr(benchmark_rates_dir)
    df_manual = get_manually_downloaded_overnight_rfr(benchmark_rates_dir)

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

    return df_appended

__all__ = [
    "H_process_overnight_rfr"
]
