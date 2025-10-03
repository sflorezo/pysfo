#---- aggregate yield series

def get_fred_yieldseries(fred_api_dir):

    import os
    import pandas as pd
    from functools import reduce

    # List of required CSV files
    required_files = [
        "FEDFUNDS.csv", "EFFR.csv", "DGS1MO.csv", "DGS3MO.csv", "DGS1.csv",
        "DGS3.csv", "DGS5.csv", "DGS7.csv", "DGS10.csv", "DGS20.csv",
        "DGS30.csv", "SOFR.csv", "RIFSPPFAAD30NB.csv", "SOFR30DAYAVG.csv",
        "RIFSPPFAAD90NB.csv", "SOFR90DAYAVG.csv", "RRPONTSYAWARD.csv",
        "BAMLC0A3CA.csv", "BAMLH0A0HYM2.csv"
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
    
