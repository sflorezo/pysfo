#%%========== helper objects ==========%%#

column_rename_map = {
    "Frequency": "frequency_label",
    "Issuer residence": "issuer_res_label",
    "Issuer nationality": "issuer_nat_label",
    "Issuer sector - immediate borrower": "issuer_bus_imm_label",
    "Issuer sector - ultimate borrower": "issuer_bus_ult_label",
    "Issue market": "market_label",
    "Issue type": "issue_type_label",
    "Issue currency group": "issue_cur_group_label",
    "Issue currency": "issue_cur_label",
    "Original maturity": "issue_or_mat_label",
    "Remaining maturity": "issue_re_mat_label",
    "Rate type": "issue_rate_label",
    "Default risk (for future expansion)": "issue_risk_label",
    "Collateral type (for future expansion)": "issue_col_label",
    "Measure": "measure_label",
}


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
        raise FileNotFoundError(_msg)

    # rename

    df.rename(columns = column_rename_map, inplace = True)
    df.columns = df.columns.str.lower()

    # fix formats

    _numeric_vars = ["value"]
    _date_vars = ["period"]
    
    for var in _numeric_vars:
        df[var] = pd.to_numeric(df[var], errors = "coerce")

    for var in _date_vars:
        df[var] = pd.to_datetime(df[var], errors = "coerce")

    df["value"] = df["value"] * 1e6 # documentation states raw data is in millions of dollars

    # return

    return df

__all__ = [
    "get"
]