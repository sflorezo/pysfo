def get_client():

    from fredapi import Fred
    import os

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise EnvironmentError("FRED_API_KEY environment variable is not set. Please set it before using FRED API features.")
    return Fred(api_key = api_key)

def fetch_fred_series_list(series_list: list, save_dir: str):
    
    import pandas as pd

    fred = get_client()

    for series in series_list:

        print(series)
        data = fred.get_series(series)
        data = pd.DataFrame(data).reset_index()
        data.columns = ["date", series]
        data.to_csv(f'{save_dir}/{series}.csv', index = False)
