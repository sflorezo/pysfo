#%%

from pathlib import Path
from ..params.fx_forwards_list import fx_forwards_prices
import pandas as pd

# from pysfo.basic import *
# fx_forwards_raw_path = Path("/storage/Dropbox/80_data/raw/fx_forwards")

#%% ========== helper functions ========== %%#

def process_bloomberg_file(
        file_path : Path, 
        contract_id : str,
        source : str,
):

    ######
    # file_path = fx_forwards_raw_path / "EURUSD_FWDS_TN_01jan2005_31dec2010.xlsx"
    # contract_id = "USD_1W_FORWARD"
    # source = "bloomberg"
    ######

    df = pd.read_excel(file_path, sheet_name = "Values", skiprows = 0)

    df["source"] = source
    df["ticker"] = df.columns[1]
    df = df.iloc[6:,:].reset_index(drop = True)
    df = df.rename(columns={
        df.columns[0]: "date",
        df.columns[1]: "pips",
    })

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["pips"] = pd.to_numeric(df["pips"], errors = "coerce")
    df["contract"] = contract_id

    return df

#%% ========== ========== %%#

def process_fx_forwards(fx_forwards_raw_path):
    
    df = pd.concat([
        pd.concat([
                pd.concat([
                    pd.concat([
                        process_bloomberg_file(
                            file_path = fx_forwards_raw_path / file, 
                            contract_id = ccy_pair + "_" + maturity,
                            source = info["source"]
                        )
                        for file in files["filenames"]
                        ])
                    for files in info["files"]
                ], axis = 0)
                for maturity, info in contract_dict.items()
            ]
        , axis = 0)
        for ccy_pair, contract_dict in fx_forwards_prices.items()
    ])

    df = df.drop_duplicates(subset = ["date", "contract"])
    df = df.sort_values(by = ["contract", "date"])

    return df

    