#%%

import pysfo.pulldata as pysfo_pull
from setpaths import *
import pandas as pd
import country_converter as coco
from matplotlib import pyplot as plt
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

cc = coco.CountryConverter()

#%%========== set env path and keys ==========%%

home_dotenv = os.path.expanduser("~/.env")  
load_dotenv(home_dotenv)

pysfo_pull.set_api_keys(api_name = "fred", api_key = os.getenv("FRED_API_KEY"))
pysfo_pull.set_api_keys(api_name = "openai", api_key = os.getenv("OPENAI_API_KEY"))

#%%========== test pull data ==========%%

#---- see all current data pulls

print("\n#===== See all current data pulls =====#\n")

print(pysfo_pull.__all__)

#---- cmns

print("\n#===== Try CMNS =====#\n")

try :
    cmns = pysfo_pull.cmns.get_restated_matrices(cmns_path = f"{raw}/cmns")
    print(cmns.head(10))
except :
    print("CMNS data missing.")

#---- efa_row

print("\n#===== Try EFA ROW =====#\n")


levels = ['agg', 'regions', 'disagg']

for l in levels:
    
    try :
        print(f"\n#---- {l}\n")
        efa = pysfo_pull.efa_row.get_efa_row(efa_row_dir = f"{raw}/efa_row", level = f"{l}")
        print(efa.head(4))

    except:
        print(f"EFA row level {l} missing")

#---- exchangerates

print("\n#===== Try FRB H10 Exchange rates =====#\n")

try :
    er = pysfo_pull.exchangerates.get_exchangerates(FRB_H10_dir = f"{raw}/FRB_H10")
    print(er.head(4))

except :
    print(f"FRB H10 Exchange rates are missing")

#---- fetch_fred_api

print("\n#===== Try FRED API =====#\n")

try :
    try_list = "DGS1"
    pysfo_pull.fetch_fred_api.fetch_fred_series_list(series_list = [try_list], save_dir = f"{temp}")
    fred_pull = pd.read_csv(f"{temp}/DGS1.csv")
    print(fred_pull.head(4))
except :
    print(f"fetch_fred_api is returning error")

#---- fof

print("\n#===== Try FRED FOF =====#\n")

try :
    get_series = ["LM713061103", "LM883164115", "FL313161105", "FL313161110"]
    fof = pysfo_pull.fof.get_fof_series(fred_flowoffunds_dir = f"{raw}/fred_flowoffunds", series = get_series, frequency = "Q")
    print(fof.head(4))
except :
    print(f"fof not uploading succesfully")

#---- fomc_dates

print("\n#===== Try FOMC dates =====#\n")


try :
    fomc = pysfo_pull.fomc_dates.get_fomc_meeting_dates(FOMC_meeting_dates_path = f"{raw}/FOMC_meeting_dates")
    print(fomc.head(4))
except :
    pass
  
#---- fred

print("\n#===== Try FRED downloaded data =====#\n")


try :
    yields = pysfo_pull.fred.get_fred_yieldseries(fred_api_dir = f"{raw}/fred_api")
    print(yields.tail(4))
except :
    print(f" yields not uploading succesfully")

#---- other

print("\n#===== Try OTHER series =====#\n")

try :
    print("Not yet implemented.")
except :
    pass




# %%
