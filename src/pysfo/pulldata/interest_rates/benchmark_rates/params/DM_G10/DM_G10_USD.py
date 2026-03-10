#%%

usd_rfr = [
    {
        "description": "SOFR (Secured Overnight Financing Rate)",
        "name_simplified": "SOFR",
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
        "name_simplified": "LIBOR",
        "rate_type": "traded, unsecured",
        "source": "manual",
        "source_params": {
            "file_format" : "json",
            "file_names": ["DM_G10_USD_LIBOR_MacroMicro.json"],
        },
        "obtained": True,
        "available": True,
        "notes": "USD LIBOR was discontinued in June 2023 (not 2018 — panel bank submissions ceased then but the formal discontinuation came later). Replaced by SOFR as the preferred RFR for USD. Historical data may still be useful for legacy contract analysis.",
    }
]
# %%
