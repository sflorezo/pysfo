overnight_rfr = {

    "DM_G10": {

        "USD": {
            "description": "SOFR (Secured Overnight Financing Rate)",
            "name_simplified" : "SOFR",
            "source": "lseg_data",
            "source_params": {
                "ric": "USDSOFR=",
            },
            "obtained" : True,
        },

        "EUR": {
            "description": "€STR (Euro Short-Term Rate)",
            "name_simplified" : "ESTR",
            "source": "lseg_data",
            "source_params": {
                "ric": "EUROSTR=",
            },
            "obtained" : True,
        },

        "GBP": {
            "description": "SONIA (Sterling Overnight Index Average)",
            "name_simplified" : "SONIA",
            "source": "lseg_data",
            "source_params": {
                "ric": "SONIAOSR=",
            },
            "obtained" : True,
        },

        "JPY": {
            "description": "TONAR (Tokyo Overnight Average Rate)",
            "name_simplified" : "TONAR",
            "source": "lseg_data",
            "source_params": {
                "ric": "JPONMU=RR",
            },
            "obtained" : True,
        },

        "CHF": {
            "description": "SARON (Swiss Average Rate Overnight)",
            "name_simplified" : "SARON",
            "source": "lseg_data",
            "source_params": {
                "ric": "SARON.S",
            },
            "obtained" : True,
        },

        "CAD": {
            "description": "CORRA (Canadian Overnight Repo Rate Average)",
            "name_simplified" : "CORRA",
            "source": "lseg_data",
            "source_params": {
                "ric": "CORRA=",
            },
            "obtained" : True,
        },

        "AUD": {
            "description": "AONIA (Australian Overnight Index Average)",
            "name_simplified" : "AONIA",
            "source": "manual",
            "source_params": {
                "file_format" : "xlsx",
                "file_names": ["AUD_f01d.xlsx"],
            },
            "obtained" : True,
        },

        "NZD": {
            "description": "NZONIA (New Zealand Overnight Index Average)",
            "name_simplified" : "NZONIA",
            "source": "manual",
            "source_params": {
                "file_format" : "xlsx",
                "file_names": ["NZD_2018_2026_hb2-daily-close.xlsx", "NZD_1985_2017_hb2-daily-close.xlsx"],
            },
            "obtained" : True,
        },

        "NOK": {
            "description": "NOWA (Norwegian Overnight Weighted Average)",
            "name_simplified" : "NOWA",
            "source": "manual",
            "source_params": {
                "file_format" : "csv",
                "file_names": ["NOK_SHORT_RATES.csv"],
            },
            "obtained" : True,
        },

        "SEK": {
            "description": "SWESTR (Swedish Krona Short-Term Rate)",
            "name_simplified" : "SWESTR",
            "source": "lseg_data",
            "source_params": {
                "ric": "SWESTR=RIKS",
            },
            "obtained" : True,
        },
    },


    "EM_CORE": {

        "BRL": {
            "description": "SELIC Overnight Rate",
            "name_simplified": "SELIC",
            "source": "lseg_data",
            "source_params": {
                "ric": "BRSELICD=CBBR",
            },
            "obtained" : True,
        },

        "MXN": {
            "description": "Tasa de Fondeo Bancario",
            "name_simplified": "TIIE",
            "source": None,
            "source_params": {
                "ric": "MXTIIED=RR",
            },
            "obtained" : False,
        },

        "ZAR": {
            "description": "ZARONIA (South African Rand Overnight Index Average)",
            "name_simplified": "ZARONIA",
            "source": None,
            "source_params": {
                "ric": "ZARONIA=",
            },
            "obtained" : False,
        },

        "TRY": {
            "description": "CBRT Overnight Repo Rate",
            "name_simplified": "CBRT",
            "source": None,
            "source_params": {
                "ric": "TRYREPO=",
            },
            "obtained" : False,
        },

        "PLN": {
            "description": "WIBOR (Warsaw Interbank Offered Rate)",
            "name_simplified": "WIBOR",
            "source": "lseg_data",
            "source_params": {
                # "ric": "POLONIA=", # Polonia is post 2025, there are some new reggulations on face
                "ric": "WIPLNOND="
            },
            "obtained" : True,
        },

        "HUF": {
            "description": "HUFONIA (Hungarian Overnight Index Average)",
            "name_simplified": "HUFONIA",
            "source": "lseg_data",
            "source_params": {
                "ric": "HUFONIA=",
            },
            "obtained" : True,
        },

        "CZK": {
            "description": "CZEONIA (Czech Overnight Index Average)",
            "name_simplified": "CZEONIA",
            "source": "lseg_data",
            "source_params": {
                "ric": "CZEONIA=",
            },
            "obtained" : True,
        },

        "CLP": {
            "description": "BCCh Overnight Repo Rate",
            "name_simplified": "CLP-TNA",
            "source": None,
            "source_params": {
                "ric": "CLPPCONDP=",
            },
            "obtained" : False,
        },

        "COP": {
            "description": "BanRep Overnight Repo Rate",
            "name_simplified": "COLREPO",
            "source": None,
            "source_params": {
                "ric": "COLREPO=",
            },
            "obtained" : False,
        },
    },


    "EM_NDF": {

        "INR": {
            "description": "MIBOR Overnight",
            "source": None,
            "source_params": {
                "ric": "INRMIBOR=",
            },
            "obtained" : False,
        },

        "KRW": {
            "description": "KOFR (Korea Overnight Financing Repo Rate)",
            "source": None,
            "source_params": {
                "ric": "KOFR=",
            },
            "obtained" : False,
        },

        "IDR": {
            "description": "Indonesia Overnight Interbank Rate",
            "source": None,
            "source_params": {
                "ric": "IDRON=",
            },
            "obtained" : False,
        },

        "PHP": {
            "description": "BSP Overnight Reverse Repo Rate",
            "source": None,
            "source_params": {
                "ric": "PHPREPO=",
            },
            "obtained" : False,
        },

        "MYR": {
            "description": "Overnight Policy Rate (BNM)",
            "source": None,
            "source_params": {
                "ric": "MYROPR=",
            },
            "obtained" : False,
        },

        "TWD": {
            "description": "Taiwan Interbank Overnight Call Loan Rate",
            "source": None,
            "source_params": {
                "ric": "TWDCALL=",
            },
            "obtained" : False,
        },

        "CNY": {
            "description": "SHIBOR Overnight / DR007",
            "source": None,
            "source_params": {
                "ric": "SHIBORON=",
            },
            "obtained" : False,
        },
    },


    "EM_FRONTIER": {

        "EGP": {
            "description": "CBE Overnight Corridor Rate",
            "source": None,
            "source_params": {
                "ric": "EGPREPO=",
            },
            "obtained" : False,
        },

        "NGN": {
            "description": "Nigerian Overnight Interbank Rate",
            "source": None,
            "source_params": {
                "ric": "NGAON=",
            }
            ,
            "obtained" : False,
        },

        "PKR": {
            "description": "SBP Overnight Repo Rate",
            "source": None,
            "source_params": {
                "ric": "PKRREPO=",
            },
            "obtained" : False,
        },

        "KZT": {
            "description": "Kazakhstan Overnight Repo Rate",
            "source": None,
            "source_params": {
                "ric": "KZTON=",
            },
            "obtained" : False,
        },

        "GHS": {
            "description": "Ghana Overnight Interbank Rate",
            "source": None,
            "source_params": {
                "ric": "GHAON=",
            },
            "obtained" : False,
        },
    }
}