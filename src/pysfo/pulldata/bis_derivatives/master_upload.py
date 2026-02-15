#%%========== data retriever ==========%%#

#---- get OTC

def get_OTC(DER_TYPE):

    import pandas as pd
    import pysfo.pulldata as pysfo_pull
    from .params import bis_OTCderivativesOutstanding_fetch_root_name
    
    # pysfo_pull.set_data_path("/storage/Dropbox/80_data/raw")

    ########
    # dataset = "OTC"
    # DER_TYPE = "A"
    #########

    #---- get data

    bis_otc = pysfo_pull.get_data_path() / "bis_derivatives_OTC_outstanding"

    try :
        _file = bis_otc / (bis_OTCderivativesOutstanding_fetch_root_name.format(DER_TYPE = DER_TYPE) + ".csv")
        df = pd.read_csv(_file, dtype = str)
    except FileNotFoundError:

        _msg = (
            f"BIS OTC Derivatices file for {DER_TYPE} not found.\n" \
            "Try running fetch_and_save_bisOTCderivatives_by_der_type() first."
        )
        raise FileNotFoundError(_msg)
    
    df.columns = df.columns.str.lower()

    return df

#---- get ETF

__all__ = [
    "get_OTC"
]