fx_forwards_prices = {
    "EURUSD" : {
        "TN" : {
            "source" : "bloomberg",
            "files" : [{
                "from" : "2001-01-01",
                "to" : "2026-03-05",
                "filenames" : [
                    "EURUSD_FWDS_TN_01jan2000_04jan2005.xlsx",
                    "EURUSD_FWDS_TN_01jan2005_31dec2010.xlsx",
                    "EURUSD_FWDS_TN_01jan2011_01jan2020.xlsx",
                    "EURUSD_FWDS_TN_01jan2020_06mar2026.xlsx"
                ]
            }]   
        },
        "1W" : {
            "source" : "bloomberg",
            "files" : [{
                "from" : "2020-01-01",
                "to" : "2026-03-05",
                "filenames" : ["EURUSD_FWDS_1WEEK_01jan2020_05mar2026.xlsx"]
            }]   
        },
        "1M" : {
            "source" : "bloomberg",
            "files" : [{
                "from" : "2020-01-01",
                "to" : "2026-03-05",
                "filenames" : ["EURUSD_FWDS_1MONTH_01jan2020_05mar2026.xlsx"]
            }]   
        }
    }
}
