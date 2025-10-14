
def fix_EMU(df_er):
    
    raise ValueError("This function is not yet implemented.")

    import country_converter as coco
    import pandas as pd
    import pysfo.pulldata as pysfo_pull
    from pysfo.basic import statatab
    cc = coco.CountryConverter()

    ###
    # from pysfo.pulldata.imf_ifs.upload_after_fetch.master_upload import get
    # subdata = "Exchange_Rates"
    # INDICATOR = "EDNE_USD_XDC_RATE"
    # FREQ = "M"
    # df_er = get(subdata, INDICATOR, FREQ)
    ###

    EMU = pysfo_pull.geo_globals()["EMU_MEMBERS"]
    EMU = pd.DataFrame({"cty_iso3" : cc.convert(EMU, to = "ISO3")})

    df_check = df_er.merge(EMU, on = "cty_iso3", how = "outer", indicator = True, validate = "m:1")

    mask = df_check["_merge"] == "right_only"
    df_check[["cty_iso3", "cty_name"]].loc[mask, :]

    if "right_only" in statatab(df_er["_merge"])["Value"].values:
        raise ValueError("Error when correcting exchange rates. Please check.")
    




    reporting = pysfo_pull.imfIFS.check_reporting(
        subdata = "Exchange_Rates",
        INDICATOR = "EDNE_USD_XDC_RATE",
        FREQ = "M",
        report_percen = None,
        start_date = "1970-01-01"
    )

    reporting.to_csv("D:/Dropbox/80_data/temp/misc/check.csv")