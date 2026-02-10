#%%========== data retriever ==========%%#

def get(REF_AREA, FREQ):

    import pandas as pd
    import pysfo.pulldata as pysfo_pull
    from .params import bis_dss_fetch_root_name
    
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    ########
    # REF_AREA = "US"
    # FREQ = "Q"
    #########
    
    bis_dss = pysfo_pull.get_data_path() / "bis_dss"

    FREQ = [FREQ] if type(FREQ) == str else FREQ

    try :
        _file = bis_dss / (bis_dss_fetch_root_name.format(REF_AREA = REF_AREA) + ".csv")
        df = pd.read_csv(_file, dtype = str)
    except FileNotFoundError:

        _msg = (
            f"BIS DSS file for {REF_AREA} not found.\n" \
            "Try running fetch_and_save_bisDSS_by_ref_area() first."
        )
        raise FileNotFoundError()
    df.columns = df.columns.str.lower()

    # keep indicator

    return df

__all__ = [
    "get"
]