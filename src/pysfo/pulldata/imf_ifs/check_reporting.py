#%%========== helper objects ==========%%#

ignore = ['G7',
    'G20',
    'Curacao & St. Maarten',
    'Yemen, P.D. Rep.',
    'Yemen Arab Rep.',
    'Central Bank West Africa',
    'Emerging and Developing Europe',
    'Other Holders',
    'EPU/EF',
    'All Countries and Country Groups',
    'All Countries',
    'All Country Groups',
    'All Countries Published',
    '1C_EMU',
    'Middle East and Central Asia',
    'National Accounts',
    'Non SRF countries',
    'SRF Countries',
    'SRF',
    'SRF',
    'IBRD',
    'ECB',
    'BIS',
    'ECCB',
    'AMF',
    'BCEAO',
    'BEAC',
    'CEMAC',
    'Eastern Caribbean Currency Union',
    'WAEMU',
    'IDB',
    'Western Hemisphere',
    'Anguilla',
    'Netherlands Antilles',
    'Former Czechoslovakia',
    'Countries Home Portal Presentation',
    'East Germany',
    'Eastern Africa deprecated',
    'Sub-Saharan Africa',
    'French Guiana',
    'Guernsey',
    'Gibraltar',
    'Guadeloupe',
    'Jersey',
    'Africa',
    'Oceania',
    'Americas',
    'Asia',
    'Europe',
    'Martinique',
    'Montserrat',
    'Namibia',
    'Saint Pierre and Miquelon',
    'Belgo-Luxembourg Economic Union',
    'Reunion',
    'Former U.S.S.R.',
    'Taiwan, Province of China',
    'Euro area',
    'World',
    'All Countries, excluding the IO',
    'IMF Member Countries',
    'Advanced Economies',
    'Emerging and Developing Countries',
    'Developing Asia',
    'Yemen',
    'Former Yugoslavia'
]

#%%========== check reporting code ==========%%#

def check_reporting(subdata, INDICATOR, FREQ, report_percen = None, start_date = None, end_date = None, REF_AREA_all = False):
    
    from pysfo.pulldata.imf_ifs.master_upload import get
    import pysfo.pulldata as pysfo_pull
    import pandas as pd
    import numpy as np
    import country_converter as coco
    from pysfo.basic import statatab
    from pathlib import Path
    import os
    import textwrap
    from pysfo.basic import silent_call
    import re


    # pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")
    cc = coco.CountryConverter()

    df = get(subdata, INDICATOR, FREQ, silent = True)

    if start_date is not None:
        df = df[df["period"] >= start_date]
    if end_date is not None:
        df = df[df["period"] <= end_date]

    current_dir = os.path.dirname(__file__)
    file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
    json_metadata_path = os.path.join(current_dir, file)

    all_ref_areas = pysfo_pull.dbnomicstools.get_filters(json_metadata_path, filter = "REF_AREA")

    # all ref_areas and periods
    
    all_ref_areas = all_ref_areas[["VALUE", "DESCRIPTION_TEXT"]]
    all_ref_areas.columns = ["ref_area", "ref_area_desc"]

    all_periods = df["period"].unique()

    skeleton = (
        pd.MultiIndex.from_product(
            [all_ref_areas["ref_area"], all_periods],
            names=["ref_area", "period"]
        )
        .to_frame(index=False)
        .merge(all_ref_areas, on="ref_area", how="left")
        .sort_values(["ref_area", "period"])
        .reset_index(drop=True)
    )
    skeleton = skeleton[["ref_area", "ref_area_desc", "period"]]

    # merge size

    try :
        size_orderer = pysfo_pull.wbWDI.get(indicator = "NY.GDP.MKTP.CD", silent = True)
    except FileNotFoundError as e:
        _message = textwrap.dedent("""\
        Need to pull indicator 'NY.GDP.MKTP.CD' from wbWDI before creating counrty report check. 
        For more info, please see the documentation on wbWDI
                                
        instructions = pysfo_pull.wbWDI.print_instructions()
        print(instructions)
        """)
        raise ValueError(_message)

    size_orderer = size_orderer[
        (size_orderer["cty_iso2"] != "not found")
        | (size_orderer["cty_iso2"].isna())
    ]

    # size_orderer_merge_checker = (
    #     size_orderer[["cty_iso2", "cty_name"]]
    #     .drop_duplicates()
    # )

    size_orderer = (
        size_orderer
        .dropna(subset = ["value"])
        .sort_values(["cty_iso2", "period"])
        .groupby("cty_iso2", as_index=False)
        .tail(1)[["cty_iso2", "period", "value"]]
        .reset_index(drop = True)
        .rename(columns = {"cty_iso2" : "ref_area",
                        "period" : "gdp_last_available_period",
                        "value" : "last_gdp"})
    )

    size_orderer = all_ref_areas.merge(
        size_orderer[["ref_area", "gdp_last_available_period", "last_gdp"]], 
        on = "ref_area",
        how = "outer",
        indicator = True,
        validate = "1:1"
    )
    
    see_left_only = size_orderer.loc[size_orderer["_merge"] == "left_only", "ref_area"]
    mask = all_ref_areas["ref_area"].isin(see_left_only)
    check = all_ref_areas.loc[mask, "ref_area_desc"]

    for ig in ignore:

        check = check[~(check.str.match(ig, case = False))]

    if len(check):
        raise ValueError(f"Error constructing reporter_check for {INDICATOR} ({FREQ}). Please check construction.")
    
    _merge_results = statatab(size_orderer["_merge"])
    size_orderer["gdp_available"] = np.where(size_orderer["_merge"] == "left_only", 0, 1)
    size_orderer.drop(columns = "_merge", inplace = True)
    
    skeleton_w_gdp = (
        pd.MultiIndex.from_product(
            [size_orderer["ref_area"], all_periods],
            names=["ref_area", "period"]
        )
        .to_frame(index=False)
        .merge(size_orderer, on="ref_area", how="left")
        .sort_values(["ref_area", "period"])
        .reset_index(drop=True)
    )
    
    skeleton_w_gdp["last_gdp"] = skeleton_w_gdp["last_gdp"].fillna(0)

    # merge df to check

    h_df = df.merge(skeleton_w_gdp, how = "outer", on = ["period", "ref_area"], indicator = True, validate = "1:1")

    _merge_results = statatab(h_df["_merge"])
    if "left_only" in [col["Value"] for _, col in _merge_results.iterrows() if col["Count"] != 0]:
        raise ValueError(f"Error constructing reporter_check for {INDICATOR} ({FREQ}). Please check construction.")
    
    h_df["_merge"] = np.where(h_df["_merge"] == "both", 1, 0)
    h_df["decade"] = (h_df["period"].dt.year // 10) * 10
    h_df = h_df.sort_values(by = ["ref_area", "period"])
    
    if FREQ == "A":
        to_year = 1
    elif FREQ == "Q":
        to_year = 4
    elif FREQ == "M":
        to_year = 12

    # check country ids (from full-sized skeleton) are non-missing before collapsing

    if True in ((h_df["ref_area"].isna()) | (h_df["period"].isna())):
        raise ValueError(f"Error constructing reporter_check for {INDICATOR} ({FREQ}). Please check construction.")
    
    # collapse at decade and find number of years reporting

    not_to_pivot = ["gdp_last_available_period", "last_gdp", "gdp_available"]
    meta = h_df[["ref_area", "ref_area_desc"] + not_to_pivot].drop_duplicates()
    
    h_df = (
        h_df
        .groupby(["ref_area", "ref_area_desc", "decade"])
        .agg(
            merged_years = ("_merge", lambda x : x.sum() / to_year),
            gdp_last_available_period = ("gdp_last_available_period", "first"),
            last_gdp = ("last_gdp", "first"),
        )
        .reset_index()
        .pivot_table(
            index = ["ref_area", "ref_area_desc"],
            columns = "decade",
            values = "merged_years",
        ).reset_index()
    )
    h_df = h_df.merge(meta, on = ["ref_area", "ref_area_desc"], how = "left", validate = "1:1", indicator = True)

    _merge_results = statatab(h_df["_merge"])
    h_df.drop(columns = "_merge", inplace = True)
    _checker = (
        _merge_results
        .loc[_merge_results["Value"] == "both", "Percentage"]
        .loc[0]
    )
    if _checker != 100:
        raise ValueError(f"Error constructing reporter_check for {INDICATOR} ({FREQ}). Please check construction.")

    decadevars = h_df.filter(regex = r"^\d{4}$").columns
    h_df = h_df[list(h_df.drop(columns = decadevars).columns) + list(decadevars)]

    h_df[decadevars] = h_df[decadevars].apply(lambda x : x.fillna(0))
    h_df[decadevars] = h_df[decadevars].round(2)

    h_df["perc_reported"] = h_df[decadevars].sum(axis=1) / h_df[decadevars].sum(axis=1).max()
    h_df = pd.concat([
        h_df.drop(columns = decadevars),
        h_df[decadevars]
    ], axis = 1)
    h_df["perc_reported"] = h_df["perc_reported"].round(2)

    if report_percen is not None:
        drop = h_df["perc_reported"] >= report_percen
        h_df = h_df[~drop]

    # merge iso3 if available

    all_ref_areas = h_df["ref_area"].unique()
    iso_3 = silent_call(cc.convert, all_ref_areas, src = "ISO2", to = "ISO3", verbose = False) 
    iso_3 = pd.DataFrame({"ref_area" : all_ref_areas, "cty_iso3" : iso_3})

    h_df = h_df.merge(iso_3, on = "ref_area", how = "left", validate = "1:1")
    h_df = h_df[["cty_iso3"] + list(h_df.drop(columns = "cty_iso3").columns)]

    # final formats

    h_df = h_df.sort_values(by = "last_gdp", ascending = False).reset_index(drop = True)
    h_df.columns.name = None
    h_df["last_gdp"] = h_df["last_gdp"] / 1e9
    h_df["last_gdp"] = h_df["last_gdp"].round(2)
    h_df["gdp_last_available_period"] = h_df["gdp_last_available_period"].dt.year

    # rename and output
    
    h_df.rename(columns = {
        "gdp_last_available_period" : "GDP Last Reporting Year",
        "last_gdp" : "Last GDP Reported (USD Billion)",
        "perc_reported" : "Percentage of Periods Reported"
    }, inplace = True)

    # print sample selection

    _message = textwrap.dedent("""\
    \n Reporting REF_AREA with GDP available in IMF_IFS. To see all possible 
    REF_AREA, please set flag REF_AREA_all = True.
    """)

    if REF_AREA_all == False:
        print(_message)
        h_df = h_df.loc[h_df["gdp_available"] == 1, :]

    h_df = h_df.drop(columns = "gdp_available")

    return h_df