#%%========== helper functions ==========%%#

def _rename_dataset(df):

    df = df.rename(columns = {
        "country (label)" : "country_label",
        "indicator (label)" : "indicator_label",
    })

    return df

def _fix_country_identifiers():

    ignore = ["Yemen", "Namibia"]

    rename_preprocess_country = {
        "Curacao & St. Maarten" : "St. Maarten"
    }

    rename_ctyname_to_iso2 = {
    }

    rename_ctyname_to_iso3 = {
        # "Euro area (Member States and Institutions of the Euro Area) changing composition": "EMU",
        # "Netherlands Antilles": "ANT",
        # "East Germany" : "DDR"
    }

    rename_ctyname_long_to_short = {
    }

    return_ = (
        ignore,
        rename_preprocess_country,
        rename_ctyname_to_iso2,
        rename_ctyname_to_iso3, 
        rename_ctyname_long_to_short
    )
    
    return return_

#%%========== data retriever ==========%%#

def get(indicator, frequency = None):

    import pandas as pd
    import country_converter as coco
    import numpy as np
    import pysfo.pulldata as pysfo_pull

    # pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")
    
    cc = coco.CountryConverter()
    wb_wdi_dir = pysfo_pull.get_data_path() / "wb_wdi"

    # subdata = subdata.replace(" ", "_")

    df = pd.read_csv(f"{wb_wdi_dir}/{indicator}.csv", dtype = str)
    df = _rename_dataset(df)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")

    # keep indicator
    indicator = [indicator] if type(indicator) == str else indicator
    keep = (
        (df["indicator"].isin(indicator))
    )
    df = df.loc[keep, :]

    if frequency is not None:
        frequency = [frequency] if type(frequency) == str else frequency
        keep = (
            (df["frequency"].isin(frequency))
        )
        df = df.loc[keep, :]

    #--- fix country names

    (
        ignore,
        *_
    ) = _fix_country_identifiers()

    # ignore

    for ig in ignore:

        df = df[~(df["country_label"].str.match(ig, case = False))]

    # check country: keep for future checks

    if df[df["country"].isna()]["country_label"].nunique() > 0:
        raise ValueError(f"There are missing REF_AREA in {indicator}. Please check.")

    # store master country

    df["master_country"] = df["country"]
    df = df.rename(columns = {"country" : "cty_iso3"})

    # for checks of data names
    # h_tmp = df.copy()
    # h_tmp["min_date"] = df.groupby("country_label")["period"].transform("min")
    # h_tmp["max_date"] = df.groupby("country_label")["period"].transform("max")
    # h_tmp[["series_name", "country_label", "min_date", "max_date"]].sort_values(by = "country_label").drop_duplicates().to_csv(f"{temp}/check.csv")

    if len(df) == 0:
        raise ValueError(f"Series {indicator} not found in raw. Please check.")

    keepcols = [
        "period",
        "value",
        "frequency",
        "master_country",
        "cty_iso3",
        "indicator",
        "country_label",
        "indicator_label"
    ]

    df = df[keepcols]
    df["country_label"] = (
        df["country_label"]
        .str.replace(",", "", regex=False)
        .str.strip()
        .str.replace(r"^0+", "", regex=True)
    )

    # HERE.

    # pre-rename checks: Leave this as future check
    # df[df["country_label"].str.match("yemen", case = False)]
    # df[df["cty_name"].str.match("yemen", case = False)]
    # df.to_csv(f"{temp}/check.csv")

    #--- rename country names and add iso ids

    (
        _,
        rename_preprocess_country,
        rename_ctyname_to_iso2,
        rename_ctyname_to_iso3, 
        rename_ctyname_long_to_short
    ) = _fix_country_identifiers()

    df["cty_name"] = df["country_label"]

    # direct renaming

    for old, new in rename_preprocess_country.items():
        mask = df["country_label"] == old
        df["country_label"] = np.where(mask, new, df["country_label"])

    df["cty_iso2"] = cc.pandas_convert(series = df["country_label"], to = 'ISO2')  

    for old, new in rename_ctyname_to_iso2.items():
        df["cty_iso2"] = np.where(df["country_label"] == old, new, df["cty_iso2"])

    for old, new in rename_ctyname_to_iso3.items():
        df["cty_iso3"] = np.where(df["country_label"] == old, new, df["cty_iso3"])

    for old, new in rename_ctyname_long_to_short.items():
        df["cty_name"] = np.where(df["cty_name"] == old, new, df["cty_name"])

    # Leave this as future check.
    # df[["country_label", "cty_name", "cty_iso2", "cty_iso3"]].drop_duplicates().sort_values(by = ["country_label"]).to_csv(f"{temp}/check.csv")
    # mask = (
    #     (df["cty_name"].isna()) |
    #     (df["cty_name"] == "not found")
    # )
    # df.loc[mask, :]

    # check duplicates: Leave this as future check
    # (
    #     df[df[["period", "cty_name"]].duplicated()]
    #     .sort_values(by = ["cty_name", "period"])
    #     .to_csv(f"{temp}/check.csv")
    # )

    # keep final data

    keep_cols = [
        "indicator_label",
        "indicator",
        "master_country",
        "cty_iso3",
        "cty_iso2",
        "cty_name",
        "frequency",
        "period",
        "value"
    ]

    df = df[keep_cols].sort_values(by = ["cty_iso3", "period"])
    df.rename(columns = {"master_country" : "country"}, inplace = True)

    if True in df[["period", "country"]].duplicated().values:
        raise ValueError(f"Duplicates found while cleaning {indicator}. Please check.")

    # set final formats

    df["value"] = pd.to_numeric(df["value"], errors = "coerce")
    df["period"] = pd.to_datetime(df["period"], errors = "coerce")

    return df

__all__ = [
    "get"
]