#%%========== dict of currently available cleaned datasets ==========%%#

available_datasets = {
    "yieldseries" : {
        "desc" : "Bunch of very useful daily yield series retrieved from FRED.",
        "needs" : [            
            "FEDFUNDS", "EFFR", "DGS1MO", "DGS3MO", "DGS1",
            "DGS3", "DGS5", "DGS7", "DGS10", "DGS20",
            "DGS30", "SOFR", "RIFSPPFAAD30NB", "SOFR30DAYAVG",
            "RIFSPPFAAD90NB", "SOFR90DAYAVG", "RRPONTSYAWARD",
            "BAMLC0A3CA", "BAMLH0A0HYM2"
        ]
    }
}

#%%========== aggregate yield series ==========%%#

class FREDcleaned:

    @staticmethod
    def about():

        return (
            "Retrieves cleaned FRED datasets constructed from individual FRED Series, after downloading these from the FRED API."
        )

    @staticmethod
    def print_instructions():

        from .config import get_data_path

        fred_api_dir = get_data_path() / "fred_api"

        return (
            f"To use this method, store the FRED-downloaded series directly as \n\n"
            f"{fred_api_dir}\\<SERIESID>.csv \n\n"
            f"Each series should be stored as a separate CSV file, with the name of the file being the series ID. \n"
            f"For example, the series 'DGS3' should be stored as 'DGS3.csv'. \n"
            f"Each csv file should have the following columns: \n"
            f"date, <SERIESID> \n"
            "For example, the file 'DGS3.csv' should have the columns 'date' and 'DGS3'"            
        )
    
    @staticmethod
    def available_datasets():
        
        return available_datasets

    class _Getter:
        
        @staticmethod
        def yieldseries():

            import os
            import pandas as pd
            from functools import reduce
            from .config import get_data_path

            # set path
            fred_api_dir = get_data_path() / "fred_api"

            # List of required CSV files
            required_files = [
                series + ".csv"
                for series in
                available_datasets["yieldseries"]["needs"]
            ]
            
            # Check for the existence of each required CSV file
            missing_files = [file for file in required_files if not os.path.exists(f"{fred_api_dir}/{file}")]

            if missing_files:
                raise FileNotFoundError(f"The following required files are missing in '{fred_api_dir}': {', '.join(missing_files)}")

            # treasuries and monetary policy

            fedfunds = pd.read_csv(f"{fred_api_dir}/FEDFUNDS.csv")
            effr = pd.read_csv(f"{fred_api_dir}/EFFR.csv")
            y008 = pd.read_csv(f"{fred_api_dir}/DGS1MO.csv")
            y025 = pd.read_csv(f"{fred_api_dir}/DGS3MO.csv")
            y1 = pd.read_csv(f"{fred_api_dir}/DGS1.csv")
            y2 = pd.read_csv(f"{fred_api_dir}/DGS2.csv")
            y3 = pd.read_csv(f"{fred_api_dir}/DGS3.csv")
            y5 = pd.read_csv(f"{fred_api_dir}/DGS5.csv")
            y7 = pd.read_csv(f"{fred_api_dir}/DGS7.csv")
            y10 = pd.read_csv(f"{fred_api_dir}/DGS10.csv")
            y20 = pd.read_csv(f"{fred_api_dir}/DGS20.csv")
            y30 = pd.read_csv(f"{fred_api_dir}/DGS30.csv")
            sofr = pd.read_csv(f"{fred_api_dir}/SOFR.csv")
            cp30d = pd.read_csv(f"{fred_api_dir}/RIFSPPFAAD30NB.csv")
            sofr30d = pd.read_csv(f"{fred_api_dir}/SOFR30DAYAVG.csv")
            cp90d = pd.read_csv(f"{fred_api_dir}/RIFSPPFAAD90NB.csv")
            sofr90d = pd.read_csv(f"{fred_api_dir}/SOFR90DAYAVG.csv")
            rrp = pd.read_csv(f"{fred_api_dir}/RRPONTSYAWARD.csv")

            # ig
            
            ig = pd.read_csv(f"{fred_api_dir}/BAMLC0A3CA.csv")

            # hy

            hy = pd.read_csv(f"{fred_api_dir}/BAMLH0A0HYM2.csv")

            # consolidate

            dfs = [fedfunds, effr, y008, y025, y1, y2, y3, y5, y7, y10, y20, y30, ig, hy, cp90d, 
                sofr90d, cp30d, sofr30d, sofr, rrp]
            names = ["fedfunds", 'effr', 'y008', 'y025', 'y1', 'y2', 'y3', 'y5', 'y7', 'y10', 'y20', 'y30', 'ig', 'hy', "cp90d", 
                    "sofr90d", "cp30d", "sofr30d", "sofr", "rrp"]

            for df, name in zip(dfs, names):
                df.columns = ['date', name.lower()]

                if not pd.api.types.is_datetime64_any_dtype(df["date"]):
                    df["date"] = pd.to_datetime(df["date"])

            df = reduce(lambda left, right: pd.merge(left, right, on='date', how='outer'), dfs)
            df = df.sort_values("date").ffill()
            df[names] = df[names] / 100

            # Create a complete date range from the min to max date in the DataFrame
            full_date_range = pd.date_range(start=df["date"].min(), end =  df["date"].max(), freq='D')
            df = df.set_index("date").reindex(full_date_range).rename_axis("date").reset_index()
            df.sort_values("date", inplace=True)
            df = df.ffill()

            return df
        
    get = _Getter()
    
