gpb_rfr = [
    #---- overnight
    {
        "description": "SONIA (Sterling Overnight Index Average)",
        "name_simplified" : "SONIA",
        "tenor" : "ON",
        "rate_type": "traded, unsecured",
        "source": "lseg_data",
        "source_params": {
            "ric": "SONIAOSR=",
        },
        "obtained" : True,
        "available" : True,
        "notes" : "",
    },
    #---- term rates
    {   
        "description": "GBP LIBOR (Sterling London Interbank Offered Rate)",
        "name_simplified" : "GBP_LIBOR",
        "tenor" : "3M",
        "rate_type" : "quoted, unsecured",
        "source": "manual",
        "source_params": {
            "file_names": ["DM_G10_3M_LIBOR_MICROMICRO_USD_EUR_GBP_JPY.json"],
        },
        "obtained" : True,
        "available" : True,
        "notes" : "GBP LIBOR discontinued December 31, 2021. Replaced by SONIA.",
    },
]