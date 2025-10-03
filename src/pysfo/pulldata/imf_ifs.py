def _rename_dataset(df):

    df = df.rename(columns = {
        "Indicator" : "indicator_label",
    })

    return df

def print_subdata_fullstr(imf_ifs_dir, subdata):
    
    import pandas as pd

    df = pd.read_csv(f"{imf_ifs_dir}/{subdata}.csv", dtype = str)
    df = _rename_dataset(df)
    df.columns = df.columns.str.lower()
    
    IND_CODE = pd.DataFrame(df["indicator"])
    IND_LAB_FULL = pd.DataFrame(df["indicator_label"])
    FREQ = pd.DataFrame(df["freq"])

    IND_LABS = df["indicator_label"].str.split(",", expand = True).apply(lambda col : col.str.strip())
    IND_LABS.columns = ["indicator_label_" + str(i) for i, _ in enumerate(IND_LABS, start = 1)]

    IND = pd.concat([IND_CODE, FREQ, IND_LAB_FULL, IND_LABS], axis = 1)
    IND = IND.drop_duplicates()
    IND = IND.sort_values(by = [
        "indicator_label_2",
        "indicator_label_3",
        "indicator_label_4", 
        "indicator_label_5"
    ]).reset_index(drop = True)

    for lab2 in IND["indicator_label_2"].unique():
        print(f"\n#========== {lab2} ==========#\n")
        mask = IND["indicator_label_2"] == lab2
        inds = IND.loc[mask, "indicator"]
        freqs = IND.loc[mask, "freq"]
        inds_labs = IND.loc[mask, "indicator_label"]
        for ind, freq, main_lab in zip(inds, freqs, inds_labs):
            ind = f"({freq}, {ind})"
            print(f"{ind  :<30}" + main_lab)

    pass

def _fix_country_identifiers(subdata):

    if subdata == "International_Investment_Positions":

        rename_ctyname_to_iso3 = {
            "Euro area (Member States and Institutions of the Euro Area) changing composition": "EMU",
        }

        rename_ctyname_to_iso2 = {
            "Namibia": "NA",
        }

        rename_iso3_to_ctyname = {
            "EMU" : "Euro Area"
        }

        return rename_ctyname_to_iso2, rename_ctyname_to_iso3, rename_iso3_to_ctyname


def get_imf_ifs(imf_ifs_dir, subdata, INDICATOR, FREQ):
    
    import pandas as pd
    import country_converter as coco
    import numpy as np

    cc = coco.CountryConverter()
    
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
        raise ValueError(f"Series {INDICATOR} not found in {subdata}. Please use 'print_subdata_fullstr' to see available series.")

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

    df["cty_iso3"] = cc.pandas_convert(series = df["reference_area"], to = 'ISO3')  

    rename_ctyname_to_iso2, rename_ctyname_to_iso3, rename_iso3_to_ctyname = _fix_country_identifiers(subdata)

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

    return df

__all__ = [
    "print_subdata_fullstr",
    "get_imf_ifs"
]