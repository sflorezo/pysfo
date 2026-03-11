#%%========== helper codes ==========%%#

DM_G10 = {
    "AUDUSD": {"yfinance": "AUDUSD=X", "fred": "DEXUSAL"},
    "EURUSD": {"yfinance": "EURUSD=X", "fred": "DEXUSEU"},
    "GBPUSD": {"yfinance": "GBPUSD=X", "fred": "DEXUSUK"},
    "NZDUSD": {"yfinance": "NZDUSD=X", "fred": "DEXUSNZ"},
    "USDCAD": {"yfinance": "USDCAD=X", "fred": "DEXCAUS"},
    "USDCHF": {"yfinance": "USDCHF=X", "fred": "DEXSZUS"},
    "USDJPY": {"yfinance": "USDJPY=X", "fred": "DEXJPUS"},
    "USDNOK": {"yfinance": "USDNOK=X", "fred": "DEXNOUS"},
    "USDSEK": {"yfinance": "USDSEK=X", "fred": "DEXSDUS"},
}

EM_CORE = {
    "USDBRL": {"yfinance": "USDBRL=X", "fred": "DEXBZUS"},
    "USDMXN": {"yfinance": "USDMXN=X", "fred": "DEXMXUS"},
    "USDZAR": {"yfinance": "USDZAR=X", "fred": "DEXSFUS"},
    "USDTRY": {"yfinance": "USDTRY=X", "fred": None},
    "USDPLN": {"yfinance": "USDPLN=X", "fred": None},
    "USDHUF": {"yfinance": "USDHUF=X", "fred": None},
    "USDCZK": {"yfinance": "USDCZK=X", "fred": None},
    "USDCLP": {"yfinance": "USDCLP=X", "fred": None},
    "USDCOP": {"yfinance": "USDCOP=X", "fred": None},
}

EM_NDF = {
    "USDINR": {"yfinance": "USDINR=X", "fred": "DEXINUS"},
    "USDKRW": {"yfinance": "USDKRW=X", "fred": "DEXKOUS"},
    "USDIDR": {"yfinance": "USDIDR=X", "fred": None},
    "USDPHP": {"yfinance": "USDPHP=X", "fred": None},
    "USDMYR": {"yfinance": "USDMYR=X", "fred": "DEXMAUS"},
    "USDTWD": {"yfinance": "USDTWD=X", "fred": None},
    "USDCNY": {"yfinance": "USDCNY=X", "fred": "DEXCHUS"},
}

EM_FRONTIER = {
    "USDEGP": {"yfinance": "USDEGP=X", "fred": None},
    "USDNGN": {"yfinance": "USDNGN=X", "fred": None},
    "USDPKR": {"yfinance": "USDPKR=X", "fred": None},
    "USDKZT": {"yfinance": "USDKZT=X", "fred": None},
    "USDGHS": {"yfinance": "USDGHS=X", "fred": None},
}

#%%========== consolidate ==========%%#

ccy_pairs = {

    # G10 liquid currency pairs
    "DM_G10" : DM_G10,

    # EM core liquid currency pairs
    "EM_CORE" : EM_CORE,

    # EM NDF (non-deliverable forward) currency pairs (capital controls)
    "EM_NDF" : EM_NDF,
    
    # EM frontier markets
    "EM_FRONTIER" : EM_FRONTIER

}