jpy_rfr = [
    #---- overnight
    {
        "description": "TONAR (Tokyo Overnight Average Rate)",
        "name_simplified" : "TONAR",
        "tenor" : "ON",
        "rate_type": "traded, unsecured",
        "source": "lseg_data",
        "source_params": {
            "ric": "JPONMU=RR",
        },
        "obtained" : True,
        "available" : True,
        "notes" : "",
    },
    #---- term rates
    {   
        "description": "JPY LIBOR (Japanese Yen London Interbank Offered Rate)",
        "name_simplified" : "JPY_LIBOR",
        "tenor" : "3M",
        "rate_type" : "quoted, unsecured",
        "source": "manual",
        "source_params": {
            "file_names": ["DM_G10_3M_LIBOR_MICROMICRO_USD_EUR_GBP_JPY.json"],
        },
        "obtained" : True,
        "available" : True,
        "notes" : "JPY LIBOR discontinued December 31, 2021. Replaced by TONA.",
    },
]