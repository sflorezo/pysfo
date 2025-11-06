#%%========== helper functions ==========%%#

def _rename_dataset(df):

    df = df.rename(columns = {
        "Indicator" : "indicator_label",
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

def get(subdata, INDICATOR, FREQ, silent = False):

    import pandas as pd
    import country_converter as coco
    import numpy as np
    import pysfo.pulldata as pysfo_pull
    from pysfo.basic import silent_call, flatten_list
    from pysfo.pulldata.exceptions import SeriesNotFoundError
    import textwrap

    # pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")
    # subdata = "Liabilities"
    # INDICATOR = "ILPD_BP6_USD"
    # FREQ = "A"
    
    cc = coco.CountryConverter()
    upload_dir = pysfo_pull.get_data_path() / "imf_bop"
    
    INDICATOR = [INDICATOR] if type(INDICATOR) == str else INDICATOR

    FREQ = [FREQ] if type(FREQ) == str else FREQ

    subdata = subdata.replace(" ", "_")

    df = pd.read_csv(f"{upload_dir}/{subdata}.csv", dtype = str)
    df = _rename_dataset(df)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")

    # raise error if required indicators not in dataset

    not_found_list = [series for series in INDICATOR if series not in df["indicator"].unique()]
    not_found_list_ = "\n".join(not_found_list)
    if len(not_found_list) > 0:
        _error_msg = textwrap.dedent(f"""\
        Series not found in subdata = '{subdata}' with FREQ = '{FREQ}'. Series not found are:
       {not_found_list_}""")
        raise SeriesNotFoundError(not_found_list = not_found_list, message = _error_msg)
    
    # keep indicator
    
    keep = (
        (df["freq"].isin(FREQ))
        & (df["indicator"].isin(INDICATOR))
    )
    df = df.loc[keep, :]

    #--- fix country names

    (
        ignore,
        *_
    ) = _fix_country_identifiers()

    # ignore

    for ig in ignore:

        df = df[~(df["reference_area"].str.match(ig, case = False))]

    # check ref_area: keep for future checks

    if df[df["ref_area"].isna()]["reference_area"].nunique() > 0:
        raise ValueError(f"There are missing REF_AREA in {INDICATOR}. Please check.")

    # store master ref_area

    df["master_ref_area"] = df["ref_area"]
    df = df.rename(columns = {"ref_area" : "cty_iso2"})

    # for checks of data names
    # h_tmp = df.copy()
    # h_tmp["min_date"] = df.groupby("reference_area")["period"].transform("min")
    # h_tmp["max_date"] = df.groupby("reference_area")["period"].transform("max")
    # h_tmp[["series_name", "reference_area", "min_date", "max_date"]].sort_values(by = "reference_area").drop_duplicates().to_csv(f"{temp}/check.csv")
    
    keepcols = [
        "period",
        "value",
        "freq",
        "master_ref_area",
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

    # pre-rename checks: Leave this as future check
    # df[df["reference_area"].str.match("yemen", case = False)]
    # df[df["cty_name"].str.match("yemen", case = False)]

    #--- rename country names and add iso ids

    (
        _,
        rename_preprocess_ref_area, 
        rename_ctyname_to_iso2,
        rename_ctyname_to_iso3, 
        rename_ctyname_long_to_short
    ) = _fix_country_identifiers()

    df["cty_name"] = df["reference_area"]
    
    # direct renaming

    for old, new in rename_preprocess_ref_area.items():
        mask = df["cty_name"] == old
        df["cty_name"] = np.where(mask, new, df["cty_name"])

    for old, new in rename_ctyname_to_iso2.items():
        df["cty_iso2"] = np.where(df["country_label"] == old, new, df["cty_iso2"])
    
    df["cty_iso3"] = silent_call(cc.pandas_convert, series=df["cty_name"], to='ISO3', verbose = not silent)
    
    for old, new in rename_ctyname_to_iso3.items():
        df["cty_iso3"] = np.where(df["cty_name"] == old, new, df["cty_iso3"])

    for old, new in rename_ctyname_long_to_short.items():
        df["cty_name"] = np.where(df["cty_name"] == old, new, df["cty_name"])

    # Leave this as future check.
    # df[["reference_area", "cty_name", "cty_iso2", "cty_iso3"]].drop_duplicates().sort_values(by = ["reference_area"]).to_csv(f"{temp}/check.csv")
    # mask = (
    #     (df["reference_area"].isna()) 
    #     | (df["cty_name"].isna()) 
    #     | (df["cty_iso2"].isna())
    #     | (df["cty_iso3"].isna())
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
        "master_ref_area",
        "cty_iso3",
        "cty_iso2",
        "cty_name",
        "freq",
        "period",
        "value"
    ]

    df = df[keep_cols].sort_values(by = ["cty_iso3", "period"])
    df.rename(columns = {"master_ref_area" : "ref_area"}, inplace = True)

    if True in df[["period", "ref_area", "indicator"]].duplicated().values:
        raise ValueError(f"Duplicates found while cleaning {subdata}. Please check.")

    # set final formats

    df["value"] = pd.to_numeric(df["value"], errors = "coerce")
    df["period"] = pd.to_datetime(df["period"], errors = "coerce")

    return df

__all__ = [
    "get"
]