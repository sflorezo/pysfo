overnight_rfr = {

    "DM_G10": {

        "USD": {
            "description": "SOFR (Secured Overnight Financing Rate)",
            "name_simplified" : "SOFR",
            "rate_type" : "traded, secured",
            "source": "lseg_data",
            "source_params": {
                "ric": "USDSOFR=",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "EUR": {
            "description": "€STR (Euro Short-Term Rate)",
            "name_simplified" : "ESTR",
            "rate_type" : "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                "ric": "EUROSTR=",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "GBP": {
            "description": "SONIA (Sterling Overnight Index Average)",
            "name_simplified" : "SONIA",
            "rate_type": "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                "ric": "SONIAOSR=",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "JPY": {
            "description": "TONAR (Tokyo Overnight Average Rate)",
            "name_simplified" : "TONAR",
            "rate_type": "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                "ric": "JPONMU=RR",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "CHF": {
            "description": "SARON (Swiss Average Rate Overnight)",
            "name_simplified" : "SARON",
            "rate_type": "traded, secured",
            "source": "lseg_data",
            "source_params": {
                "ric": "SARON.S",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "CAD": {
            "description": "CORRA (Canadian Overnight Repo Rate Average)",
            "name_simplified" : "CORRA",
            "rate_type": "traded, secured",
            "source": "lseg_data",
            "source_params": {
                "ric": "CORRA=",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "AUD": {
            "description": "AONIA (Australian Overnight Index Average)",
            "name_simplified" : "AONIA",
            "rate_type": "traded, unsecured",
            "source": "manual",
            "source_params": {
                "file_format" : "xlsx",
                "file_names": ["AUD_f01d.xlsx"],
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "NOK": {
            "description": "NOWA (Norwegian Overnight Weighted Average)",
            "name_simplified" : "NOWA",
            "rate_type": "traded, unsecured",
            "source": "manual",
            "source_params": {
                "file_format" : "csv",
                "file_names": ["NOK_SHORT_RATES.csv"],
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "SEK": {
            "description": "SWESTR (Swedish Krona Short-Term Rate)",
            "name_simplified" : "SWESTR",
            "rate_type": "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                "ric": "SWESTR=RIKS",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "NZD": {
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
    },

    "EM_CORE": {

        "BRL": {
            "description": "SELIC Overnight Rate",
            "name_simplified": "SELIC",
            "rate_type": "policy",
            "source": "lseg_data",
            "source_params": {
                "ric": "BRSELICD=CBBR",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "MXN": {
            "description": "Tasa de Fondeo Bancario",
            "name_simplified": "TIIE",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "MXTIIED=RR",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "ZAR": {
            "description": "ZARONIA (South African Rand Overnight Index Average)",
            "name_simplified": "ZARONIA",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "ZARONIA=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "TRY": {
            "description": "CBRT Overnight Repo Rate",
            "name_simplified": "CBRT",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "TRYREPO=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "PLN": {
            "description": "WIBOR (Warsaw Interbank Offered Rate)",
            "name_simplified": "WIBOR",
            "rate_type": "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                # "ric": "POLONIA=", # Polonia is post 2025, there are some new reggulations on face
                "ric": "WIPLNOND="
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "HUF": {
            "description": "HUFONIA (Hungarian Overnight Index Average)",
            "name_simplified": "HUFONIA",
            "rate_type": "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                "ric": "HUFONIA=",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "CZK": {
            "description": "CZEONIA (Czech Overnight Index Average)",
            "name_simplified": "CZEONIA",
            "rate_type": "traded, unsecured",
            "source": "lseg_data",
            "source_params": {
                "ric": "CZEONIA=",
            },
            "obtained" : True,
            "available" : True,
            "notes" : "",
        },

        "CLP": {
            "description": "BCCh Overnight Repo Rate",
            "name_simplified": "CLP-TNA",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "CLPPCONDP=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "COP": {
            "description": "BanRep Overnight Repo Rate",
            "name_simplified": "COLREPO",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "COLREPO=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },
    },


    "EM_NDF": {
        "INR": {
            "description": "MIBOR Overnight",
            "name_simplified": "MIBOR",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "INRMIBOR=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "KRW": {
            "description": "KOFR (Korea Overnight Financing Repo Rate)",
            "name_simplified": "KOFR",
            "rate_type": "traded, secured",
            "source": None,
            "source_params": {
                "ric": "KOFR=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "IDR": {
            "description": "Indonesia Overnight Interbank Rate",
            "name_simplified": "IONIA",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "IDRON=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },
        "PHP": {
            "description": "BSP Overnight Reverse Repo Rate",
            "name_simplified": "BSPRR",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "PHPREPO=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "MYR": {
            "description": "Overnight Policy Rate (BNM)",
            "name_simplified": "OPR",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "MYROPR=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "TWD": {
            "description": "Taiwan Interbank Overnight Call Loan Rate",
            "name_simplified": "TWDCALL",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "TWDCALL=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "CNY": {
            "description": "SHIBOR Overnight / DR007",
            "name_simplified": "DR007",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "SHIBORON=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },
    },


    "EM_FRONTIER": {

        "EGP": {
            "description": "CBE Overnight Corridor Rate",
            "name_simplified": "CBE",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "EGPREPO=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "NGN": {
            "description": "Nigerian Overnight Interbank Rate",
            "name_simplified": "NGAON",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "NGAON=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "PKR": {
            "description": "SBP Overnight Repo Rate",
            "name_simplified": "PKRRR",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "PKRREPO=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "KZT": {
            "description": "Kazakhstan Overnight Repo Rate",
            "name_simplified": "KZTON",
            "rate_type": "policy",
            "source": None,
            "source_params": {
                "ric": "KZTON=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },

        "GHS": {
            "description": "Ghana Overnight Interbank Rate",
            "name_simplified": "GHAON",
            "rate_type": "traded, unsecured",
            "source": None,
            "source_params": {
                "ric": "GHAON=",
            },
            "obtained" : False,
            "available" : False,
            "notes" : "",
        },
    }
}
