#%%========== useful objects ==========%%#

# Enhanced Financial Accounts Tables labels

efa_tables = {

    "table1": {
        "short": "frgn_holdings_lt_total",
        "long": "Foreign Residents' Holdings of Total U.S. Long-term Securities by Country"
    },
    "table1a": {
        "short": "frgn_holdings_lt_treas",
        "long": "Foreign Residents' Holdings of U.S. Long-term Treasury Securities by Country"
    },
    "table1b": {
        "short": "frgn_holdings_lt_agency",
        "long": "Foreign Residents' Holdings of U.S. Long-term Agency Bonds by Country"
    },
    "table1c": {
        "short": "frgn_holdings_lt_corp",
        "long": "Foreign Residents' Holdings of U.S. Long-term Corporate and Other Bonds by Country"
    },
    "table1d": {
        "short": "frgn_holdings_stock",
        "long": "Foreign Residents' Holdings of U.S. Corporate Stocks by Country"
    },
    "table1e": {
        "short": "frgn_holdings_st_treas",
        "long": "Memo: Foreign Residents' Holdings of U.S. Short-term Treasury Securities by Country"
    },
    "table2": {
        "short": "us_holdings_lt_total",
        "long": "U.S. Residents' Holdings of Total Foreign Long-term Securities by Country"
    },
    "table2a": {
        "short": "us_holdings_lt_bonds",
        "long": "U.S. Residents' Holdings of Foreign Long-term Bonds by Country"
    },
    "table2b": {
        "short": "us_holdings_stock",
        "long": "U.S. Residents' Holdings of Foreign Corporate Stocks by Country"
    }

}

#%%========== EFARow class retriever ==========%%#

class EFARow:
    @staticmethod
    def about():

        return (
            "Enhanced Financial Accounts of the USA (Rest of the World). Provide monthly, country-level detail on international portfolio investment holdings of long-term securities, complementing the aggregated quarterly data in table L.133 (“Rest of the World”) of the main Financial Accounts."
        )

    @staticmethod
    def print_instructions():

        from .config import get_data_path

        efa_row_dir = get_data_path() / "efa_row"

        return (
            f"To use this dataset, download the EFA Rest of the World data from the FRB. You should manually download each of the following individual tables: \n\n"
            'international-portfolio-investment-table1-historical.csv\n'
            'international-portfolio-investment-table1a-historical.csv\n'
            'international-portfolio-investment-table1b-historical.csv\n'
            'international-portfolio-investment-table1c-historical.csv\n'
            'international-portfolio-investment-table1d-historical.csv\n'
            'international-portfolio-investment-table1e-historical.csv\n'
            'international-portfolio-investment-table2-historical.csv\n'
            'international-portfolio-investment-table2a-historical.csv\n'
            'international-portfolio-investment-table2b-historical.csv\n\n'
            'And store them in the following directory:\n'
            f"{efa_row_dir}\n"
        )

    def get(level = "agg"):

        from .config import get_data_path
        import pandas as pd
        import country_converter as coco
        import warnings
        import os
        import numpy as np
        
        cc = coco.CountryConverter()

        if level not in ["agg", "regions", "disagg"]:
            raise ValueError("level must be one of 'agg', 'regions', 'disagg'")

        # Define the file path
        efa_row_dir = get_data_path() / "efa_row"
    
        # Check if the file exists
        if not os.path.exists(efa_row_dir):
            raise FileNotFoundError(f"The required path '{efa_row_dir}' does not exist. Please check the instructions for downloading the data.'")
        
        # List all required CSV file paths
        required_files = [
            efa_row_dir / f"international-portfolio-investment-{tab}-historical.csv"
            for tab in efa_tables.keys()
        ]

        # Check for missing files
        missing_files = [file for file in required_files if not os.path.exists(file)]

        if missing_files:
            raise FileNotFoundError(f"The following required files are missing in '{efa_row_dir}': {', '.join(missing_files)}")


        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            all_data = []

            for tab, seriesname in efa_tables.items():

                df = pd.read_csv(f"{efa_row_dir}/international-portfolio-investment-{tab}-historical.csv", dtype = str)
                df.iloc[:,1:] = df.iloc[:,1:].apply(lambda x : pd.to_numeric(x, errors = "coerce"))

                df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0], errors="coerce")

                df_long = df.melt(id_vars=[df.columns[0]], 
                                var_name="country", 
                                value_name="value")
                
                df.columns = df.columns.str.lower()

                df_long["series_key"] = seriesname["short"]
                df_long["series_name"] = seriesname["long"]
                
                all_data.append(df_long)

            df_all = pd.concat(all_data, ignore_index=True)
            df_all.columns = df_all.columns.str.lower()

            # get country ISO3 codes

            df_all[["country_1", "country_2", "country_3"]] = df_all["country"].str.split(";", expand = True)
            df_all[["country_1", "country_2", "country_3"]] = df_all[["country_1", "country_2", "country_3"]].apply(lambda x : x.str.strip())

            df_all = df_all.rename(columns = {"country_1" : "region"})

            df_all["cty_iso3_2"] = cc.pandas_convert(series = df_all["country_2"], to='ISO3')  
            df_all["cty_iso3_3"] = cc.pandas_convert(series = df_all["country_3"], to='ISO3')  

            rename_map = {
                "Eurozone": "EMU",
                "Non-Eurozone": "NOT_EMU",
                "French West Indies": "MTQ",
                "Netherlands Antilles": "ANT",
            }

            for old, new in rename_map.items():
                df_all["cty_iso3_2"] = np.where(df_all["country_2"] == old, new, df_all["cty_iso3_2"])

            # check mistakes

            check = df_all["cty_iso3_2"] != df_all["cty_iso3_3"]
            df_all.loc[check, ["cty_iso3_3", "cty_iso3_2"]].sort_values(by = ["cty_iso3_3", "cty_iso3_2"]).drop_duplicates()

            df_all["cty_iso3"] = ""

            mask = (df_all["cty_iso3_2"] == df_all["cty_iso3_3"])
            df_all["cty_iso3"] = np.where(mask, df_all["cty_iso3_2"], df_all["cty_iso3"])

            mask = check & (df_all["cty_iso3_3"] == "not found")
            df_all["cty_iso3"] = np.where(mask, df_all["cty_iso3_2"], df_all["cty_iso3"])

            mask = check & (df_all["cty_iso3_3"] != "not found")
            df_all["cty_iso3"] = np.where(mask, df_all["cty_iso3_3"], df_all["cty_iso3"])

            mask = (df_all["region"] == "International/Regional Organizations")
            df_all["cty_iso3"] = np.where(mask, "INT_ORG", df_all["cty_iso3"])

            mask = (df_all["region"] == "Country Unknown")
            df_all["cty_iso3"] = np.where(mask, "UNKNOWN", df_all["cty_iso3"])

            df_all["cty_iso3"] = np.where(df_all["cty_iso3"] == "not found", "", df_all["cty_iso3"])

            # Leave this as future check.
            # df_all[["country", "region", "cty_iso3"]].drop_duplicates().sort_values(by = ["cty_iso3"]).to_csv(f"{temp}/check.csv")

            df_all = df_all[["date", "country", "region", "cty_iso3", "series_key", "series_name", "value"]]

            # set final formats

            df_all = df_all.rename(columns = {"value" : "value_bn"})
            df_all["value_bn"] = pd.to_numeric(df_all["value_bn"])
            df_all["value"] = df_all["value_bn"] * 1e9
            df_all.drop(columns = "value_bn", inplace = True)

            df_all["monthly"] = df_all["date"].dt.to_period("M")
            df_all = df_all[["monthly"] + list(df_all.drop(columns = ["date", "monthly"]).columns)] 

            # return data

            if level == "agg":

                mask = df_all["country"].str.contains("Worldwide")
                df_all = df_all.loc[mask, :]
                return df_all
            
            if level == "regions":
                
                mask = (df_all["cty_iso3"] == "") | (df_all["cty_iso3"] == "INT_ORG")
                df_all = df_all[mask]

                mask = (df_all["cty_iso3"] == "INT_ORG")
                df_all["country"] = np.where(mask, "Int. Org.",df_all["country"])
                df_all["region"] = np.where(mask, "Int. Org.",df_all["region"])

                return df_all
            
            if level == "disagg":

                mask = (df_all["cty_iso3"] != "")
                df_all = df_all[mask]
                df_all["country"] = df_all["country"].str.split(";").str[1].str.strip()

                mask = (df_all["cty_iso3"] == "UNKNOWN") 
                df_all["country"] = np.where(mask, "Unknown",df_all["country"])
                df_all["region"] = np.where(mask, "Unknown",df_all["region"])

                mask = (df_all["cty_iso3"] == "INT_ORG")
                df_all["country"] = np.where(mask, "Int. Org.",df_all["country"])
                df_all["region"] = np.where(mask, "Int. Org.",df_all["region"])

                return df_all
    

    @staticmethod
    def efa_zone_labels(level = "disagg"):

        import io
        from contextlib import redirect_stdout
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            if level in ["regions", "agg"]:
                return "Column 'region' already has its values as labels."

            f = io.StringIO()
            with redirect_stdout(f):
                efa = EFARow.get(level)
        
            efa = efa[["cty_iso3", "country", "region"]].drop_duplicates()

            zone_labels = {
                row["cty_iso3"] : {
                    "label"  : row["country"],
                    "region" : row["region"]
                }
                for _, row in efa.iterrows()
            }

            return zone_labels  
