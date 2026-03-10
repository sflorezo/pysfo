#%%

eur_rfr = [
    {   
        "description": "€STR (Euro Short-Term Rate)",
        "name_simplified" : "ESTR",
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
]

# %%
