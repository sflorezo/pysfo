#%%========== data retriever ==========%%#

def get(ISSUER_RES, FREQ):

    import pandas as pd
    import pysfo.pulldata as pysfo_pull
    from .params import bis_ids_fetch_root_name
    
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    ########
    # ISSUER_RES = "US"
    # FREQ = "Q"
    #########
    
    bis_ids = pysfo_pull.get_data_path() / "bis_ids"

    FREQ = [FREQ] if type(FREQ) == str else FREQ

    try :
        _file = bis_ids / (bis_ids_fetch_root_name.format(ISSUER_RES = ISSUER_RES) + ".csv")
        df = pd.read_csv(_file, dtype = str)
    except FileNotFoundError:

        _msg = (
            f"BIS IDS file for {ISSUER_RES} not found.\n" \
            "Try running fetch_and_save_bisIDS_by_issuer_res() first."
        )
        raise FileNotFoundError()
    df.columns = df.columns.str.lower()

    # keep indicator

    return df

__all__ = [
    "get"
]