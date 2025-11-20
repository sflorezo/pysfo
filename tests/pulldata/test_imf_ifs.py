#%%========== packages ==========%%#

import pysfo.pulldata as pysfo_pull

# pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")

#%%========== tests ==========%%#

#--- imf_ifs about and documentation

def test_imf_ifs_about():
    test_message = "TRY IMF IFS DOCUMENTATION"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    about = pysfo_pull.imfIFS.about()
    print("About:\n" + "-"*len("About:") + "\n" + about + "\n")

#--- imf_ifs check_reporting

def test_imf_ifs_check_reporting():
    test_message = "TRY CHECK REPORTING OF SERIES"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    print("Data Glimpse:\n" + "-"*len("MData Glimpse:") + "\n")
    
    subdata = "International Investment Positions"
    INDICATOR = "IAPD_BP6_USD"
    FREQ = "Q"
    report = pysfo_pull.imfIFS.check_reporting(
        subdata, 
        INDICATOR, 
        FREQ,
    )
    
    print(report.tail(4))

#--- imf_ifs dbDownload

def test_imf_ifs_dbDownload():
    test_message = "TRY IMF IFS DBNOMICS DONWLOAD"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    # test main series documentation
    main_series_docs = pysfo_pull.imfIFS.dbDownload().main_series_documentation()[0:100]
    print("Main Series Documentation:\n" + "-"*len("Main Series Documentation:") + "\n" + main_series_docs + "\n")
    
    # test fetch series
    print("Test fetch series:\n" + "-"*len("Test fetch series:") + "\n")
    save_dir = "D:/Dropbox/80_data/raw/imf_ifs"
    subdata = "International Investment Positions"
    pysfo_pull.imfIFS.dbDownload().fetch_and_save_series_by_subdata(subdata, save_dir)
    print("\n")

    # test subseries documentation
    subdata = "International Investment Positions"
    subdata_docs = pysfo_pull.imfIFS.dbDownload().subseries_documentation(subdata)[0:200]
    print("Sub-Series Documentation:\n" + "-"*len("Sub-Series Documentation:") + "\n" + subdata_docs + "\n")

    # test example code
    example_code = pysfo_pull.imfIFS.dbDownload().example_code()
    print("Example code:\n" + "-"*len("Example code:") + "\n" + example_code + "\n")

#--- imf_ifs get

def test_imf_ifs_get():
    test_message = "TRY UPLOAD AFTER FETCH"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    print("Data Glimpse:\n" + "-"*len("Data Glimpse:") + "\n")
    imf_ifs = pysfo_pull.imfIFS.get(
        subdata = "Exchange_Rates",
        INDICATOR = ["EDNE_USD_XDC_RATE"],
        FREQ = "M",
        silent = True
    )
    print(imf_ifs.tail(4))

#--- imf_ifs get_dbnomics_filters

def test_imf_ifs_get_dbnomics_filters():
    test_message = "TRY PRINT INSTRUCTIONS"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    filters = pysfo_pull.imfIFS.get_dbnomics_filters()
    print("Filters Glimpse:\n" + "-"*len("Filters Glimpse:"))
    print(filters.head(10))

#--- imf_ifs print instructions

def test_imf_ifs_print_instructions():
    test_message = "TRY PRINT INSTRUCTIONS"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    instructions = pysfo_pull.imfIFS.print_instructions()
    print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")



    
