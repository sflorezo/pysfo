#%%

eur_rfr = [
    #---- overnight
    {   
        "description": "€STR (Euro Short-Term Rate)",
        "name_simplified" : "ESTR",
        "tenor" : "ON",
        "rate_type" : "traded, unsecured",
        "source": "lseg_data",
        "source_params": {
            "ric": "EUROSTR=",
        },
        "obtained" : True,
        "available" : True,
        "notes" : "Euro overnight benchmark published by ECB since October 2, 2019. Replaced EONIA. For pre-2019 history, splice with EONIA using fixed 8.5bps spread (€STR = EONIA - 0.085).",
    },
    {   
        "description": "EONIA (Euro Overnight Index Average)",
        "name_simplified" : "EONIA",
        "tenor" : "ON",
        "rate_type" : "traded, unsecured",
        "source": "manual",
        "source_params": {
            "file_format" : "xlsx",
            "file_names": ["DM_G10_EUR_EONIA_FRED.xlsx"],
        },
        "obtained" : True,
        "available" : True,
        "notes" : "Data obtained manually from FRED. Discontinued January 2022. Pre-2019 euro overnight benchmark. Panel-based rate published by ECB/EMMI. For historical studies, splice with €STR using the fixed 8.5bps spread (EONIA = €STR + 0.085).",
    },
    #---- term rates
    {   
        "description": "EURIBOR (Euro Interbank Offered Rate)",
        "name_simplified" : "EURIBOR",
        "tenor" : "3M",
        "rate_type" : "quoted, unsecured",
        "source": "manual",
        "source_params": {
            "file_names": ["DM_G10_3M_LIBOR_MICROMICRO_USD_EUR_GBP_JPY.json"],
        },
        "obtained" : True,
        "available" : True,
        "notes" : "Euro-denominated interbank offered rate published by EMMI. Available in multiple tenors (1W, 1M, 3M, 6M, 12M). Still active benchmark unlike USD/GBP LIBOR, having survived the LIBOR phase-out.",
    },
]

# %%
