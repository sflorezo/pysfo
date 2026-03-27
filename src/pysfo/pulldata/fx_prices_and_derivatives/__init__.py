from .tlaarits_panel import fxTLaarits
from .manually_downloaded import fxManuallyDownloaded
from pysfo.basic import silent_call
import pandas as pd
from typing import cast
import re

# from pysfo.pulldata.fx_prices_and_derivatives.tlaarits_panel import fxTLaarits
# from pysfo.pulldata.fx_prices_and_derivatives.manually_downloaded import fxManuallyDownloaded
# from pysfo.basic import *

#%%========== helper functions ==========%%#

def _process_manualdownload_in_tlaarits_format():

    # output handler

    output = {}

    # get manual fx forward prices

    df_manual = silent_call(fxManuallyDownloaded().get_fx_forwards_prices, verbose=False)

    # convert df_manual to df_tlaarits format 
    # (only take 1W and TN from manual, as the rest are available in TLaarits panel)

    df_manual[["ccy_pair", "maturity"]] = df_manual["contract"].str.split("_", expand=True)
    keep = df_manual["maturity"].isin(["1W", "TN"])
    df_manual = df_manual[keep]  

    df_manual["ccy"] = (
        df_manual["ccy_pair"]
        .map({
            "EURUSD": "EUR",
        })
    )

    df_manual["contract"] = (df_manual["ccy"] + "_" + "forward" + "_" + df_manual["maturity"]).str.lower()

    # output ticker map

    output["ticker_map"] = (
        df_manual[["contract", "ticker"]]
        .drop_duplicates()
        .reset_index(drop = True)
    )

    # convert to wide panel

    df_manual = df_manual[["date", "pips", "contract"]]
    df_manual = (
        df_manual
        .pivot(
            index= "date",
            columns = "contract",
            values = "pips"
        ).reset_index()
    )
    df_manual.columns.name = None

    # merge pips with spot fx to build forwards

    get_spot_ccys = list(set([col.split("_")[0] + "_spot" for col in df_manual.columns if col != "date"]))

    df_tlaarits = cast(pd.DataFrame, silent_call(fxTLaarits().get_fx_panel, verbose=False))
    spot_fx = df_tlaarits[["date"] + get_spot_ccys]

    df_manual = pd.merge(df_manual, spot_fx, on = "date", how = "left", validate = "1:1")

    # all currency pairs as foreign currency / USD:
    # -> Recall data from clean T-Laarits file is already in foreign currency per USD. Thus, I need to 
    # convert it back to USD per foreign currency to then apply pips (as pips are quoted in the same
    # BASE/QUOTE as the spot fx).

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

    keep_cols = ["date"]

    for spot_ccy in fx_per_dollar:

        pip_divisor = 100 if re.search("jpy", spot_ccy) else 10000
        ccy_fwd_pips = df_manual.columns[df_manual.columns.str.match(rf"{spot_ccy}_forward.*")]

        for col in ccy_fwd_pips:
            
            df_manual[f"{col}"] = df_manual[f"{spot_ccy}_spot"] + df_manual[col] / pip_divisor
            keep_cols.append(f"{col}")

    for spot_ccy in dollar_per_fx:
        
        pip_divisor = 10000
        ccy_fwd_pips = df_manual.columns[df_manual.columns.str.match(rf"{spot_ccy}_forward.*")]

        if f"{spot_ccy}_spot" in df_manual.columns:
            
            df_manual["_dollars_per_fx"] = 1 / df_manual[f"{spot_ccy}_spot"] # turn back to USD per unit of FX

            for col in ccy_fwd_pips:
                
                df_manual[f"{col}"] = df_manual[f"_dollars_per_fx"] + df_manual[col] / pip_divisor
                df_manual[f"{col}"] = 1 / df_manual[f"{col}"]
                keep_cols.append(f"{col}")

    df_manual = df_manual[keep_cols]

    # output fx df
    output["fx_df"] = df_manual

    return output

def _build_consolidated_fx_panel():

    # message

    _msg = (
        "Returning fully processed forward rates as FX per unit dollars.\n" \
        "Panel includes both forwards and spot rates for Advanced Economies.\n" \
        "Recall:\n" \
        "- benchmark: benchmark rates for currency.\n" \
        "- govt: Sovereign bond index.\n" \
        "- swap: Plain vainilla same currency interest rate swaps.\n" \
        "- spot: spot FX.\n" \
        "- forward: is currency FX forwards.\n" \
        "- basis: references cross-currency interest rate swaps"
    )

    print(_msg)

    # upload both tlaarits panel and manually donwloaded series

    df_tlaarits = silent_call(fxTLaarits().get_fx_panel, verbose=False)
    df_manual = _process_manualdownload_in_tlaarits_format()["fx_df"]

    df_tlaarits, df_manual = cast(pd.DataFrame, df_tlaarits), cast(pd.DataFrame, df_manual)

    df = pd.merge(df_tlaarits, df_manual, on = "date", how = "outer", validate = "1:1")
    
    ccy_order = sorted(set([col.split("_")[0] for col in df.columns if col != "date"]))
    contract_order = ['benchmark', 'govt', 'swap', 'spot', 'forward', 'basis']
    mat_order = [None, "tn", "1w", "1m", "2m", "3m", "6m", "12m", "1y", "2y", "3y", "5y", "7y", "10y"]

    ordered_cols = [
        f"{ccy}_{contract}_{mat}" if mat else f"{ccy}_{contract}"
        for ccy in ccy_order
        for contract in contract_order
        for mat in mat_order
    ]

    ordered_cols = [col for col in ordered_cols if col in df.columns]
    df = df[["date"] + ordered_cols]

    # set everything in correct units

    in_percentage_points = [
        "benchmark",
        "swap",
        "govt",
    ]

    pattern = "|".join([rf"[a-z]{{3}}_{c}" for c in in_percentage_points])
    all_matched_cols = df.columns[df.columns.str.contains(pattern, regex=True)]
    df[all_matched_cols] = df[all_matched_cols] / 100
    df = df.copy()

    # return

    return df

def _build_consolidated_fx_tickers():

    _msg = (
        "Returning raw tickers used for construction of FX panel."
    )

    print(_msg)

    tickers_tlaarits = silent_call(fxTLaarits().get_fx_tickers, verbose=False)
    tickers_manual = _process_manualdownload_in_tlaarits_format()["ticker_map"]

    all_tickers = pd.concat([tickers_manual, tickers_tlaarits], axis = 0)

    all_tickers[["_ccy", "_contracttype", "_maturity"]] = all_tickers["contract"].str.split("_", expand = True)

    ccy_order = sorted(all_tickers["_ccy"].unique())
    contract_order = ['benchmark', 'govt', 'swap', 'spot', 'forward', 'basis']
    mat_order = [None, "tn", "1w", "1m", "2m", "3m", "6m", "12m", "1y", "2y", "3y", "5y", "7y", "10y"]

    ordered_rows = [
        f"{ccy}_{contract}_{mat}" if mat else f"{ccy}_{contract}"
        for ccy in ccy_order
        for contract in contract_order
        for mat in mat_order
    ]

    ordered_rows = [row for row in ordered_rows if row in all_tickers["contract"].unique()]

    order_map = {
        row : i for i, row in enumerate(ordered_rows, start = 0)
    }

    all_tickers["_order"] = all_tickers["contract"].map(order_map)

    all_tickers = (
        all_tickers
        .sort_values(by = ["_order"])
        .filter(regex = r"^[^_]+")
        .reset_index(drop = True)
    )

    return all_tickers


#%%========== final callable module ==========%%#

class FXdata:

    @staticmethod
    def get_fx_panel():

        return _build_consolidated_fx_panel()
    
    @staticmethod
    def get_fx_tickers():
        
        return _build_consolidated_fx_tickers()


__all__ = [
    "FXdata"
]