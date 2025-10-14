#%%

from setpaths import *
import pysfo.pulldata as pysfo_pull
pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")

# %%


reporting = pysfo_pull.imfIFS.check_reporting(
    subdata = "Exchange_Rates",
    INDICATOR = "EDNE_USD_XDC_RATE",
    FREQ = "M",
    report_percen = None,
    start_date = None,
    end_date = None
)


## fast checks
FROM_OTHER_DATA = None
h_df = FROM_OTHER_DATA
to_year = FROM_OTHER_DATA

#-- run

indicator = "NY.GDP.MKTP.CD"

pysfo_pull.wbWDI.dbDownload().fetch_and_save_series_all_ctys
