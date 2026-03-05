#%%

from pysfo.pulldata.interest_rates.benchmark_rates.params.overnight_rfr import overnight_rfr
from pathlib import Path
import pandas as pd
from typing import Union

#%% ========== script settings ========== %%#

lseg_benchmark_rate_file_name = "{cty_group}_{ccy_iso3}_{benchmark_name}.csv"

#%% ========== go ========== %%#

def get_lseg_overnight_rfr(
    benchmark_rates_dir,
):

    """Get the overnight rates from LSEG Data."""

    # get the data

    overnight_rfr_lseg = [
        df.assign(
            cty_group=cty_group,
            ccy_iso3=ccy_iso3,
            name_simplified=info["name_simplified"]
        )
        for cty_group, cty_dict in overnight_rfr.items()
        for ccy_iso3, info in cty_dict.items()
        if (
            (info["source"] == "lseg_data")
            and (info["obtained"] == True)
        )
        for df in [pd.read_csv(
            benchmark_rates_dir / lseg_benchmark_rate_file_name.format(
                cty_group=cty_group,
                ccy_iso3=ccy_iso3,
                benchmark_name=info["name_simplified"]
            )
        )]
    ]

    # some fixes

    overnight_rfr_lseg = [
        df.assign(FIXING_1=df["MID_PRICE"].fillna((df["ASK"] + df["BID"]) / 2))
        if df["ccy_iso3"].iloc[0] == "PLN"
        else df
        for df in overnight_rfr_lseg
    ]
    # rename columns

    rename_dict = {
        "TRDPRC_1": "FIXING_1"
        
    }

    overnight_rfr_lseg = [
        df.rename(columns = rename_dict) for df in overnight_rfr_lseg
    ]

    # check all columns have the same information

    required_cols = {'name_simplified', 'FIXING_1', 'ccy_iso3', 'cty_group', 'Date'}

    for i, df in enumerate(overnight_rfr_lseg):
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(
                f"DataFrame {i} ({df['ccy_iso3'].iloc[0]}) is missing columns: {missing}"
            )

    overnight_rfr_lseg = [df[list(required_cols)] for df in overnight_rfr_lseg]


    # check all have daily data

    for df in overnight_rfr_lseg:
        avg_diff = pd.to_datetime(df["Date"]).diff().dt.days.mean()
        if avg_diff > 2.5:
            raise ValueError(
                f"{df['ccy_iso3'].iloc[0]}: data might not be daily (avg gap = {avg_diff:.1f} days)"
            )

    # consolidate

    df_combined = pd.concat(overnight_rfr_lseg, ignore_index=True)

    # formats

    df_combined = df_combined.rename(columns={
        "Date": "date",
        "FIXING_1": "rate",
        "ccy_iso3": "ccy",
        "cty_group": "group",
        "name_simplified": "benchmark"
    })

    df_combined["date"] = pd.to_datetime(df_combined["date"])
    df_combined["rate"] = pd.to_numeric(df_combined["rate"], errors="coerce")

    df_combined = df_combined[["date", "group", "ccy", "benchmark", "rate"]]

    return df_combined

# %%
