#%%========== helper functions ==========%%#

def _rename_dataset(df):

    df = df.rename(columns = {
        "WEO Subject" : "weo_subject_desc",
        "WEO Countries group": "weo_countries_group_desc",
        "Unit": "unit_desc"
    })

    return df

#%%========== data retriever ==========%%#

def get():
    
    import pandas as pd
    import country_converter as coco
    import numpy as np
    from pysfo.pulldata.config import get_data_path

    imf_weo_dir = get_data_path() / "imf_weo"
    
    # INDICATOR = [INDICATOR] if type(INDICATOR) == str else INDICATOR

    # FREQ = [FREQ] if type(FREQ) == str else FREQ

    df = pd.read_csv(f"{imf_weo_dir}/WEO_oct2024.csv", dtype = str)
    df = _rename_dataset(df)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("-", "_")
    df.columns = df.columns.str.replace("@", "")
   
    keepcols = [
        "period",
        "value",
        "frequency",
        "series_code",
        "series_name",
        "weo_countries_group",
        "weo_countries_group_desc",
        "weo_subject",
        "weo_subject_desc",
        "unit"
    ]

    df = df[keepcols]
    df["weo_countries_group_desc"] = (
        df["weo_countries_group_desc"]
        .str.strip()
        .str.replace(r"^0+", "", regex=True)
    )

    return df

__all__ = [
    "get"
]