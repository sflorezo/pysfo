#%%========== helper functions ==========%%#

def _rename_dataset(df):

    df = df.rename(columns = {
        "Indicator" : "indicator_label",
    })

    return df

def _fix_country_identifiers():

    rename_ctyname_to_iso3 = {
        "Euro area (Member States and Institutions of the Euro Area) changing composition": "EMU",
        "Netherlands Antilles": "ANT",
        "East Germany" : "DDR"
    }

    rename_ctyname_to_iso2 = {
        "Namibia": "NA"
    }

    rename_iso3_to_ctyname = {
        "EMU" : "Euro Area",
        "ANT" : "Netherlands Antilles",
        "DDR" : "East Germany"
    }

    tuple_cases_iso3 = {
        ("CUW", "SXM") : "CUW"
    }

    return_ = (
        rename_ctyname_to_iso2, 
        rename_ctyname_to_iso3, 
        rename_iso3_to_ctyname, 
        tuple_cases_iso3
    )

    return return_

#%%========== data retriever ==========%%#

def get(subdata, INDICATOR, FREQ):
    
    import pandas as pd
    import country_converter as coco
    import numpy as np
    from ..config import get_data_path

    cc = coco.CountryConverter()
    imf_ifs_dir = get_data_path() / "imf_ifs"
    
    INDICATOR = [INDICATOR] if type(INDICATOR) == str else INDICATOR

    FREQ = [FREQ] if type(FREQ) == str else FREQ

    df = pd.read_csv(f"{imf_ifs_dir}/{subdata}.csv", dtype = str)
    df = _rename_dataset(df)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")
    df = df.rename(columns = {"ref_area" : "cty_iso2"})
    
    keep = (
        (df["freq"].isin(FREQ))
        & (df["indicator"].isin(INDICATOR))
    )
    df = df.loc[keep, :]

    if len(df) == 0:
        raise ValueError(f"Series {INDICATOR} not found in {subdata}.")

    keepcols = [
        "period",
        "value",
        "freq",
        "cty_iso2",
        "indicator",
        "reference_area",
        "indicator_label"
    ]

    df = df[keepcols]
    df["reference_area"] = (
        df["reference_area"]
        .str.replace(",", "", regex=False)
        .str.strip()
        .str.replace(r"^0+", "", regex=True)
    )

    df["cty_iso3"] = cc.pandas_convert(series = df["reference_area"], to = 'ISO3')  

    rename_ctyname_to_iso2, rename_ctyname_to_iso3, rename_iso3_to_ctyname, tuple_cases_iso3 = _fix_country_identifiers()

    for old, new in tuple_cases_iso3.items():
        mask = df["cty_iso3"].apply(lambda x: x == list(old))
        df["cty_iso3"] = np.where(mask, new, df["cty_iso3"])

    for old, new in rename_ctyname_to_iso3.items():
        df["cty_iso3"] = np.where(df["reference_area"] == old, new, df["cty_iso3"])

    for old, new in rename_ctyname_to_iso2.items():
        df["cty_iso2"] = np.where(df["reference_area"] == old, new, df["cty_iso2"])

    df["cty_name"] = cc.pandas_convert(series = df["cty_iso3"], src = 'ISO3', to = 'name_short')

    for old, new in rename_iso3_to_ctyname.items():
        df["cty_name"] = np.where(df["cty_iso3"] == old, new, df["cty_name"])

    # Leave this as future check.
    # df[["reference_area", "cty_name", "cty_iso2", "cty_iso3"]].drop_duplicates().sort_values(by = ["cty_iso3"]).to_csv(f"{temp}/check.csv")
    # mask = (
    #     (df["reference_area"].isna()) 
    #     | (df["cty_name"].isna()) 
    #     | (df["cty_iso2"].isna())
    #     | (df["cty_iso3"].isna())
    # )
    # df.loc[mask, :]

    # keep final data

    keep_cols = [
        "indicator_label",
        "indicator",
        "cty_iso3",
        "cty_iso2",
        "cty_name",
        "freq",
        "period",
        "value"
    ]

    df = df[keep_cols].sort_values(by = ["cty_iso3", "period"])

    # set final formats

    df["value"] = pd.to_numeric(df["value"], errors = "coerce")
    df["period"] = pd.to_datetime(df["period"], errors = "coerce")

    return df

__all__ = [
    "get"
]