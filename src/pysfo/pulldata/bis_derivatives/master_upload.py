#%%========== helper objects ==========%%#

column_rename_map = {
    "Frequency": "freq_label",
    "Measure": "der_type_label",
    "Instrument": "der_instr_label",
    "Risk category": "der_risk_label",
    "Reporting country": "der_rep_cty_label",
    "Counterparty sector": "der_sector_cpy_label",
    "Counterparty country": "der_cpc_label",
    "Underlying risk sector": "der_sector_udl_label",
    "Currency leg 1": "der_curr_leg1_label",
    "Currency leg 2": "der_curr_leg2_label",
    "Maturity": "der_issue_mat_label",
    "Rating (outstanding) or settlement (FX turnover)": "der_rating_label",
    "Execution method": "der_ex_method_label",
    "Basis": "der_basis_label",
}


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

    df["value"] = df["value"] * 1e6 # rawdata seems to be in millions of USD (BIS wepbage shows 130 Tn FX derivatives for H.A.A.B.5J.A.5J.A.TO1.TO1.A.A.3.C in 2024-S2).

    return df

#---- get ETF

__all__ = [
    "get_OTC"
]