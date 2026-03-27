#%%

from pathlib import Path
import pandas as pd
from typing import Dict, cast

# from pysfo.basic import *
# fx_forwards_raw_path = Path("/storage/Dropbox/80_data/raw/fx_prices_and_derivatives")


#%% ========== helper functions ========== %%#

#---- function that gets full cleaned panel

def _process_tlaarits_fx_panel(
        file_path : Path,
):
    
    ######
    # file_path = fx_forwards_raw_path / "tlaarits_jan2026/G10_jan2026.xlsx"
    ######

    import re
    from pysfo.configs import CONFIGS
    from pysfo.basic import load_pickle, save_pickle
    from typing import cast
    import numpy as np

    output = {}

    DATA_TEMP_PATH = (CONFIGS.get("PATHS") or {}).get("DATA_TEMP_PATH")

    if DATA_TEMP_PATH and DATA_TEMP_PATH.exists():
        for attempt in range(5):
            try :

                output = load_pickle(DATA_TEMP_PATH / "final_tlaarits_output.pkl")

                return output
            
            except FileNotFoundError:
                if attempt == 0:
                    
                    df_prices = pd.read_excel(file_path, sheet_name="Clean", skiprows=0)
                    df_keys = pd.read_excel(file_path, sheet_name="G10", skiprows=0)

                    df_prices = cast(pd.DataFrame, df_prices)
                    df_prices.columns = [col if col != "Unnamed: 0" else "date" for col in df_prices.columns]

                    drop_cols = [col for col in df_prices.columns if col.startswith("Unnamed: ")]
                    df_prices.drop(columns = drop_cols, inplace=True)
                    df_keys.columns = df_keys.columns.str.lower().str.strip()
                    
                    ticker_description_list = [
                        {"ticker" : col, "desc" : desc} for col, desc in zip(df_prices.columns, df_prices.iloc[0,:])
                        if col != "date"
                    ]
                    df_prices = df_prices.iloc[1:,:]
                    df_prices["date"] = pd.to_datetime(df_prices['date']).dt.normalize()
                    df_prices = df_prices.melt(id_vars = "date", var_name = "ticker")
                    df_prices["desc"] = df_prices["ticker"].map({
                        ticker["ticker"] : ticker["desc"]
                        for ticker in ticker_description_list
                    })

                    # select interest contracts

                    # CLEAN: I decide to select all contracts.
                    # pat_curncy = re.compile(r'curncy', re.IGNORECASE)
                    # pat_fwd_spot = re.compile(r'forward|spot', re.IGNORECASE)
                    # pat_bank = re.compile(r'bank', re.IGNORECASE)

                    # is_curncy = df_prices["ticker"].str.contains(pat_curncy, na=False)
                    # is_fwd_spot = df_prices["desc"].str.contains(pat_fwd_spot, na=False)
                    # is_bank = df_prices["desc"].str.contains(pat_bank, na=False)
                    
                    is_any = [True] * len(df_prices)

                    keep_prices = (
                        is_any
                    )

                    df_prices = df_prices[keep_prices].reset_index(drop = True)

                    # merge contracts with currency identifiers and names
            
                    df_prices = pd.merge(
                        df_prices, 
                        df_keys[["ticker", "currency", "name"]],
                        on = "ticker",
                        how = "left",
                        validate = "m:1"
                    ).reset_index(drop = True)
                    
                    df_prices["currency"] = np.where(
                        df_prices["currency"].isna(), 
                        df_prices["ticker"].str[:3], 
                        df_prices["currency"]
                    )
                    
                    is_aud_swap = df_prices["ticker"].str.contains(r"^ADSWAP\d+", regex = True, na=False)

                    df_prices["currency"] = np.where(
                        is_aud_swap, 
                        "AUD", 
                        df_prices["currency"]
                    )
                    
                    not_currencies = [
                        ccy 
                        for ccy in df_prices["currency"].unique() 
                        if not ccy in df_keys["currency"].unique()
                    ]

                    if not_currencies:
                        raise ValueError("Check. Some currency was not correctly assigned")
                    
                    # add custom description for those that were not sufficiently descriptive in rawdata

                    pat_bank = re.compile(r'bank', re.IGNORECASE)

                    is_bank = df_prices["desc"].str.contains(pat_bank, na=False)

                    df_prices["_mat"] = np.where(
                        is_bank, 
                        df_prices["name"].str[-2:], 
                        np.nan
                    )
                    df_prices["desc"] = np.where(
                        is_bank,
                        "benchmark_" + df_prices["_mat"],
                        df_prices["desc"]
                    )

                    # create final contract var

                    df_prices["contract"] = df_prices["currency"] + "_" +df_prices["desc"]
                    df_prices["contract"] = df_prices["contract"].str.lower()

                    keep_vars = ["date", "ticker", "currency", "contract", "value"]
                    df_prices = df_prices[keep_vars]

                    # convert numeric vars

                    _numeric_vars = ["value"]

                    for var in _numeric_vars:

                        df_prices[var] = pd.to_numeric(df_prices[var], errors = "coerce")

                    # reset index and return FX data

                    df_prices = df_prices.reset_index(drop = True)

                    # create ticker map

                    output["ticker_map"] = df_prices[["contract", "ticker"]].drop_duplicates().reset_index(drop = True)
                    
                    # set df_prices as wide

                    df_prices = df_prices[["date", "contract", "value"]]
                    df_prices = (
                        df_prices
                        .pivot(
                            index = "date",
                            columns = "contract",
                            values = "value"
                        ).reset_index()
                    )
                    df_prices.columns.name = None

                    # all currency pairs as foreign currency / USD

                    fx_per_dollar = [
                        "cad", 
                        "chf",
                        "dkk",
                        "jpy",
                        "nok", 
                        "sek",
                    ]

                    dollar_per_fx = [
                        "aud", 
                        "eur", 
                        "gbp", 
                        "nzd"
                    ]

                    for spot_ccy in fx_per_dollar:

                        pip_divisor = 100 if re.search("jpy", spot_ccy) else 10000
                        ccy_fwd_pips = df_prices.columns[df_prices.columns.str.match(rf"{spot_ccy}_forward_.*")]

                        for col in ccy_fwd_pips:
                            
                            df_prices[col] = df_prices[f"{spot_ccy}_spot"] + df_prices[col] / pip_divisor

                    for spot_ccy in dollar_per_fx:
                        
                        pip_divisor = 10000
                        ccy_fwd_pips = df_prices.columns[df_prices.columns.str.match(rf"{spot_ccy}_forward_.*")]

                        for col in ccy_fwd_pips:
                            
                            df_prices[col] = df_prices[f"{spot_ccy}_spot"] + df_prices[col] / pip_divisor
                            df_prices[col] = 1 / df_prices[col]
                        
                        df_prices[f"{spot_ccy}_spot"] = 1 / df_prices[f"{spot_ccy}_spot"]

                    # export

                    output["fx_df"] = df_prices

                    save_pickle(output, DATA_TEMP_PATH / "final_tlaarits_output.pkl")
                
                else:
                    raise  # CSV still not found after writing it — something is wrong

    else:
        raise ValueError("DATA_TEMP_PATH not found. Please set DATA_TEMP_PATH in pysfo/configs/project_params.toml")
    

#%% ========== final functions ========== %%#


#---- function that gets all tickers


def H_get_fx_panel(file_path):


    _msg = (
        "Returning fully processed forward rates as FX per unit dollars.\n" \
        "Panel includes both forwards and spot rates for Advanced Economies.\n" \
        "Recall:\n" \
        "- basis: references cross-currency interest rate swaps\n" \
        "- benchmark: benchmark rates for currency.\n" \
        "- forward: is currency FX forwards.\n" \
        "- govt: Sovereign bond index.\n" \
        "- spot: spot FX.\n" \
        "- swap: Plain vainilla same currency interest rate swaps.\n" \
    )

    print(_msg)

    return cast(dict, _process_tlaarits_fx_panel(file_path)).get("fx_df")
            

def H_get_fx_tickers(file_path):

    _msg = (
        "Returning raw tickers used for construction of FX panel."
    )

    print(_msg)
    return cast(dict, _process_tlaarits_fx_panel(file_path)).get("ticker_map")

__all__ = [
    "H_get_fx_panel",
    "H_get_fx_tickers"
]

# %%
