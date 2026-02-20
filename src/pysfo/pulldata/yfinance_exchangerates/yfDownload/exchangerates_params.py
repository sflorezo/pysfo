
#%% ========== file names ========== %%#

yf_er_ccypair_fetch_root_name = "yf_er_fetch_{fetch_stamps}_{ccy_group}"


#%% ========== stuff used in package

available_ccy_pairs = {

    # G10 liquid currency pairs
    "DM_G10" : [
        "AUDUSD=X",
        "EURUSD=X",
        "GBPUSD=X",
        "NZDUSD=X",
        "USDCAD=X",
        "USDCHF=X",
        "USDJPY=X",
        "USDNOK=X",
        "USDSEK=X"
    ],

    # EM core liquid currency pairs
    "EM_CORE" : [
        "USDBRL=X",
        "USDMXN=X",
        "USDZAR=X",
        "USDTRY=X",
        "USDPLN=X",
        "USDHUF=X",
        "USDCZK=X",
        "USDCLP=X",
        "USDCOP=X"
    ],

    # EM NDF (non-deliverable forward) currency pairs (capital controls)
    "EM_NDF" : [
        "USDINR=X",
        "USDKRW=X",
        "USDIDR=X",
        "USDPHP=X",
        "USDMYR=X",
        "USDTWD=X",
        "USDCNY=X"
    ],

    # EM frontier markets
    "EM_FRONTIER" : [
        "USDEGP=X",
        "USDNGN=X",
        "USDPKR=X",
        "USDKZT=X",
        "USDGHS=X"
    ]

}


# %%
