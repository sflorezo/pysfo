
# FIXME: This is a temporal file. Jschreger FX data has not been implemented yet.

#%%

from pathlib import Path
import pandas as pd
from pysfo.pulldata import FXdata
from pysfo.configs import CONFIGS
from typing import cast
import re
import matplotlib.pyplot as plt
import numpy as np

# from pysfo.basic import *

#%% temporal params

# FIXME: Temporal path that will be called somhere else
file_path = Path("/storage/Dropbox/80_data/raw/fx_prices_and_derivatives/jschreger_cip_dataset/cip_dataset_v4.csv")


#%%

check_ccys = ["eur","gbp", "jpy"]

#---- myfx

my_fx = FXdata.get_fx_panel()
keep_cols =  ["date"] + [f"{ccy}_forward_3m" for ccy in check_ccys] + [f"{ccy}_spot" for ccy in check_ccys]
my_fx = my_fx[keep_cols]
my_fx = my_fx.dropna(subset = [col for col in keep_cols if col != "date"])

# for ccy in check_ccys:
#     pips_factor = 1e4 if ccy != "jpy" else 1e2
#     my_fx[f"{ccy}_F"] = my_fx[f"{ccy}_spot"] + my_fx[f"{ccy}_forward_3m"] / pips_factor

for col in check_ccys:
    plt.plot(my_fx["date"], my_fx[f"{col}_forward_3m"], label = col)
    plt.plot(my_fx["date"], my_fx[f"{col}_spot"], label = col)
    plt.legend()
    # plt.show()
    plt.close()

for ccy in check_ccys:
    my_fx[f"rho_{ccy}"] = 100*4*(my_fx[f"{ccy}_forward_3m"] - my_fx[f"{ccy}_spot"])/my_fx[f"{ccy}_spot"]    

#---- js fx data

js_fx = pd.read_csv(file_path)
keep = js_fx["tenor"] == "3m"
js_fx = js_fx[keep]
js_fx["currency"] = js_fx["currency"].str.lower()
keep_rows = js_fx["currency"].isin(check_ccys)
js_fx = js_fx[keep_rows]
js_fx["date"] = pd.to_datetime(js_fx["date"], format="%d%b%Y")
keep_cols = ["date", "tenor", "currency", "rho"]
js_fx = (
    js_fx.pivot(
        index = ["date", "tenor"],
        columns = "currency",
        values = "rho"
    )
).reset_index()


keep_dates = [
    date for date in my_fx["date"].unique()
    if
    date in js_fx["date"].unique()
]

keep_1 = my_fx["date"].isin(keep_dates)
keep_2 = js_fx["date"].isin(keep_dates)
my_fx = my_fx[keep_1]
js_fx = js_fx[keep_2]

# finally, got the correct values

for ccy in check_ccys:
    plt.title(f"{ccy.upper()} 3M Cross-Currency Basis")
    plt.plot(my_fx["date"], my_fx[f"rho_{ccy}"], label = "sf")
    plt.plot(js_fx["date"], js_fx[f"{ccy}"], label = "js")
    plt.ylabel("bps")
    plt.legend()
    plt.show()
    plt.close()    


# %%
