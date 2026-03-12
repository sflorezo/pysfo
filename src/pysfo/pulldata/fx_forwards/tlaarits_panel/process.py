#%%

from pathlib import Path
import pandas as pd

# from pysfo.basic import *
# fx_forwards_raw_path = Path("/storage/Dropbox/80_data/raw/fx_forwards")


#%% ========== helper functions ========== %%#

#---- function that gets full cleaned panel

def _process_tlaarits_fx_panel(
        file_path : Path,
):
    
    ######
    # file_path = fx_forwards_raw_path / "tlaarits_jan2026/G10_jan2026.xlsx"
    # contract_id = "USD_1W_FORWARD"
    # source = "bloomberg"
    ######

    import re
    from pysfo.configs import CONFIGS
    from typing import cast

    output = {}

    DATA_TEMP_PATH = (CONFIGS.get("PATHS") or {}).get("DATA_TEMP_PATH")

    if DATA_TEMP_PATH and DATA_TEMP_PATH.exists():
        for attempt in range(5):
            try:
                df = pd.read_csv(DATA_TEMP_PATH / "tlaarits_light.csv", dtype = str)

                df = cast(pd.DataFrame, df)
                df.columns = [col if col != "Unnamed: 0" else "date" for col in df.columns]
                colvars_desc = df.iloc[0,:].values

                keep_cols =(
                [
                    "date"
                ]
                +
                [
                    col 
                    for col, desc in zip(df.columns, colvars_desc)
                    if (
                        (re.search(r'curncy', col, re.IGNORECASE))
                        and (re.search(r'forward', desc, re.IGNORECASE))
                    )
                ]
                )

                df = df[keep_cols]
                
                oldcols = df.columns

                df.columns = df.columns.str.lower()
                df.columns = df.columns.str.replace("curncy", "").str.strip()
                df.columns = [re.sub(r'([a-zA-Z]+)(\d+m)', r'\1_\2', col) for col in df.columns]
                df = df.iloc[1:,:]
                df["date"] = pd.to_datetime(df['date']).dt.normalize()

                # convert forward numeric vars into numeric

                _numeric_vars = [col for col in df.columns if col != "date"]

                for var in _numeric_vars:

                    df[var] = pd.to_numeric(df[var], errors = "coerce")

                # reset index and return forwards data

                df = df.reset_index(drop = True)

                # create ticker map

                newcols = df.columns

                output["ticker_map"] = (
                    pd.DataFrame(
                        {
                            "column": [el for el in newcols if el != "date"],
                            "bloomberg_ticker": [el for el in oldcols if el != "date"],
                        }
                    )
                ) 

                output["fx_df"] = df

                return output

            except FileNotFoundError:
                if attempt == 0:
                    # generate the cache and let the loop retry
                    df = pd.read_excel(file_path, sheet_name="Clean", skiprows=0)
                    df.to_csv(DATA_TEMP_PATH / "tlaarits_light.csv", index=False)
                else:
                    raise  # CSV still not found after writing it — something is wrong

    else:
        raise ValueError("DATA_TEMP_PATH not found. Please set DATA_TEMP_PATH in pysfo/configs/project_params.toml")
    

#%% ========== final functions ========== %%#


#---- function that gets all tickers


def H_get_fx_forwards_prices(file_path):

    return _process_tlaarits_fx_panel(file_path)["fx_df"]

def H_get_fx_forwards_tickers(file_path):

    return _process_tlaarits_fx_panel(file_path)["ticker_map"]

    



__all__ = [
    "H_get_fx_forwards_prices",
    "H_get_fx_forwards_tickers"
]
