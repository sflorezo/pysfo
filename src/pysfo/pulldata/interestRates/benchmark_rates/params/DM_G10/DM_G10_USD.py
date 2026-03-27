#%%

usd_rfr = [
    #---- overnight
    {
        "description": "SOFR (Secured Overnight Financing Rate)",
        "name_simplified": "SOFR",
        "tenor": "ON",
        "rate_type": "traded, secured",
        "source": "lseg_data",
        "source_params": {
            "ric": "USDSOFR=",
        },
        "obtained": True,
        "available": True,
        "notes": "SOFR replaced USD LIBOR as the preferred RFR for USD markets. First published by the New York Fed in April 2018. Based on overnight Treasury repo transactions, making it secured (vs LIBOR which was unsecured). Full transition from LIBOR completed in June 2023.",
    },
    {
        "description": "USD LIBOR (London Interbank Offered Rate)",
        "name_simplified": "USD_LIBOR",
        "tenor": "ON",
        "rate_type": "traded, unsecured",
        "source": "manual",
        "source_params": {
            "file_names": ["DM_G10_USD_LIBOR_MacroMicro.json"],
        },
        "obtained": True,
        "available": True,
        "notes": "USD LIBOR was discontinued in June 2023 (not 2018 — panel bank submissions ceased then but the formal discontinuation came later). Replaced by SOFR as the preferred RFR for USD. Historical data may still be useful for legacy contract analysis.",
    },
    #---- term rates
    {   
        "description": "USD LIBOR 1M (US Dollar London Interbank Offered Rate)",
        "name_simplified" : "USD_LIBOR",
        "tenor" : "1M",
        "rate_type" : "quoted, unsecured",
        "source": "manual",
        "source_params": {
            "file_names": ["DM_G10_USD_LIBOR_MacroMicro.json"],
        },
        "obtained" : True,
        "available" : True,
        "notes" : "USD LIBOR 1M tenor discontinued June 30, 2023. Replaced by SOFR.",
    },
    {   
        "description": "USD LIBOR (US Dollar London Interbank Offered Rate)",
        "name_simplified" : "USD_LIBOR",
        "tenor" : "3M",
        "rate_type" : "quoted, unsecured",
        "source": "manual",
        "source_params": {
            "file_names": ["DM_G10_3M_LIBOR_MICROMICRO_USD_EUR_GBP_JPY.json"],
        },
        "obtained" : True,
        "available" : True,
        "notes" : "US dollar-denominated interbank offered rate published by ICE. Officially discontinued after June 30, 2023. Replaced by SOFR as the preferred USD risk-free benchmark. Historical data available pre-cessation.",
    },
]
# %%
