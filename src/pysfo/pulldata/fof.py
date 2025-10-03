#---- uploading FoF data

# fof download dict

def fof_upload_dict():

    upload_dict = {

            # L.100 Domestic Nonfinancial Sectors
            "FL383061105" : {
                "table" : "l100",
                "frequency" : "Q",
                "varname" : "dom_nofin_assets_tsys"
            },
            "LM383064105" : {
                "table" : "l100",
                "frequency" : "Q",
                "varname" : "dom_nonfin_assets_equity"
            },

            # L.210 Treasury Securities

            "FL313161105" : {
                "table" : "l210",
                "frequency" : "Q",
                "description" : "Federal government; total marketable Treasury securities; liability",
                "varname" : "market_tsy_liab_tot"
            },
            "FL313161110" : {
                "table" : "l210",
                "frequency" : "Q",
                "description" : "Federal government; total marketable Treasury bills; liability",
                "varname" : "market_tsy_liab_bills"
            },
            "LM713061103": {
                "table" : "l210",
                "frequency" : "Q",
                "description" : "Monetary authority; total Treasury securities; asset",
                "varname" : "tsy_held_in_fed_assets_total"
            },

            # L.224 Corporate Equities
            "LM893064105" : {
                "table" : "l224",
                "frequency" : "Q",
                "description" : "All sectors; corporate equities; asset",
                "varname" : "corp_equities_asset"
            },
            "LM883164115" : {
                "table" : "l224",
                "frequency" : "Q",
                "description" : "All domestic sectors; public corporate equities; liability",
                "varname" : "dom_pubcorp_eqty_liab"
            },
    }

    return upload_dict

# download all series

def get_fof_series(fred_flowoffunds_dir, series = None, frequency = "Q"):

    import pandas as pd
    import numpy as np
    import os
    from ..basic import dupli_report
    
    if series is None:
        raise ValueError("You must provide a list of series.")
    
    series = series if isinstance(series, list) else [series]
    df_merge = []
    upload_dict = fof_upload_dict()

    if True not in [s_ in upload_dict.keys() for s_ in series]:
        out_str = ""
        for key in upload_dict.keys():
            out_str += f"{key}\n"
        raise ValueError(f"No provided series in upload_dict. Currently available series are:\n{out_str}")

    for s in [s for s in series if s not in upload_dict.keys()]:
        print(f"-> Series '{s}' not found in master download dict. Continuing without this series.")

    upload_dict = {key : val for key, val in upload_dict.items() if key in series}

    freq_order = ["D", "W", "M", "Q", "Y"]
    freq_rank = {f : i for i, f in enumerate(freq_order)}

    freqs = {val["frequency"] for val in upload_dict.values()}
    lowest_possible_frequency = freq_order[max([freq_rank[freq] for freq in freqs])]

    if freq_rank[frequency] < freq_rank[lowest_possible_frequency]:
        raise ValueError(
            f"Required frequency '{frequency}' is lower resolution than lowest_possible_frequency '{lowest_possible_frequency}'."
        )

    print(f"\n -> Clean series at frequency = {frequency}:")

    datatables = set([vals["table"] for vals in upload_dict.values()])

    for table in datatables:
        
        # check if necessary files exist

        csv_path = f"{fred_flowoffunds_dir}/csv/{table}.csv"
        datadict_path = f"{fred_flowoffunds_dir}/data_dictionary/{table}.txt"

        for path in [csv_path, datadict_path]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"The required file '{path}' is missing.")

        # get series

        get_series = {key : vals for key, vals in upload_dict.items() if vals["table"] == table}

        data = pd.read_csv(f"{fred_flowoffunds_dir}/csv/{table}.csv")

        get_cols = data.columns[data.columns.str.replace(r".Q$", "", regex = True).isin(get_series.keys())]
        data = data[["date"] + list(get_cols)]

        # process datadict
        
        datadict = pd.read_table(f"{fred_flowoffunds_dir}/data_dictionary/{table}.txt", header = None)
        datadict.columns = ["variable", "description", "line", "table", "unitnotes"]
        datadict = datadict[["variable", "description", "unitnotes"]]
        datadict = datadict[datadict["variable"].isin(get_cols)].drop_duplicates()

        datadict["unit_factor"] = np.nan
        for factor, desc in zip([1e6, 1e9, 1e12], ["millions", "billions", "trillions"]):
            mask = datadict["unitnotes"].str.contains(desc, case = False, na=False)
            datadict["unit_factor"] = np.where(mask, factor, datadict["unit_factor"])

        datadict = datadict[["variable", "description", "unit_factor"]]

        # process main data

        data["date"] = pd.PeriodIndex(data["date"].str.replace(":", ""), freq = "Q").to_timestamp(how = "end").normalize()

        data = data.melt(id_vars = "date")
        data = pd.merge(data, datadict, on = "variable", how = "left", validate = "m:1")

        data["value"] = pd.to_numeric(data["value"], errors = "coerce") * data["unit_factor"]

        # fix frequency

        data = data.sort_values(by = "date", ascending = True)
        data["date"] = data["date"].dt.to_period(frequency).dt.to_timestamp(how = "end").dt.normalize() 
        data = data.groupby(["variable", "date"]).agg({"description" : "last", "value" : "last"}).reset_index()

        if True in dupli_report(data[["date", "variable"]])["Duplicated"].values:
            raise ValueError("Duplicated dates found")
        
        # rename vars
        
        data.rename(columns = {"variable" : "series"}, inplace = True)
        data["variable"] = pd.Series(np.nan, index=data.index, dtype="string")

        varname_dict = {key + ".Q" : val["varname"] for key, val in get_series.items()}
        
        for series, varname in varname_dict.items():
            mask = (data["series"] == series)
            data["variable"] = np.where(mask, varname, data["variable"])

        data = data[["date", "series", "variable", "value", "description"]]

        data["quarterly"] = data["date"].dt.to_period("Q")
        data = data[["quarterly"] + list(data.drop(columns = ["date", "quarterly"]).columns)] 

        df_merge.append(data)

    df = pd.concat(df_merge, axis = 0) 
    del df_merge

    return df
# %%
