nzd_rfr = [
    {
        "description": "NZONIA (New Zealand Overnight Index Average)",
        "name_simplified" : "NZONIA",
        "rate_type": "traded, unsecured",
        "source": "manual",
        "source_params": {
            "file_format" : "xlsx",
            "file_names": ["NZD_2018_2026_hb2-daily-close.xlsx", "NZD_1985_2017_hb2-daily-close.xlsx"],
        },
        "obtained" : True,
        "available" : False,
        "notes" : "Set as unavailable because data from the Reserve Bank of New Zealand has lots of missings for the Overnight Interbank Cash Rate after 2020. Haven't been able to understand why this is the case."
    },
]