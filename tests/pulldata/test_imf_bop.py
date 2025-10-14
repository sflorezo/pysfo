#%%========== packages ==========%%#

import pytest
import pysfo.pulldata as pysfo_pull


# pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")

#%%========== tests ==========%%#

#--- imf_bop about and documentation

def test_imf_bop_documentation():
    test_message = "TRY IMF BOP DOCUMENTATION"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    about = pysfo_pull.imfBOP.about()
    print("About:\n" + "-"*len("About:") + "\n" + about + "\n")

    instructions = pysfo_pull.imfBOP.print_instructions()
    print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")

#--- imf_bop dbDownload

def test_imf_bop_dbDownload():
    test_message = "TRY IMF BOP DBNOMICS DONWLOAD"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    # test main series documentation
    main_series_docs = pysfo_pull.imfBOP.dbDownload().main_series_documentation()[0:100]
    print("Main Series Documentation:\n" + "-"*len("Main Series Documentation:") + "\n" + main_series_docs + "\n")
    
    # test fetch series
    print("Test fetch series:\n" + "-"*len("Test fetch series:") + "\n")
    save_dir = "D:/Dropbox/80_data/raw/imf_bop"
    subdata = "Assets"
    pysfo_pull.imfBOP.dbDownload().fetch_and_save_series_by_subdata(subdata, save_dir)
    print("\n")

    # test subseries documentation
    subdata = "Assets"
    subdata_docs = pysfo_pull.imfBOP.dbDownload().subseries_documentation(subdata)[0:200]
    print("Sub-Series Documentation:\n" + "-"*len("Sub-Series Documentation:") + "\n" + subdata_docs + "\n")

    # test example code
    example_code = pysfo_pull.imfBOP.dbDownload().example_code()
    print("Example code:\n" + "-"*len("Example code:") + "\n" + example_code + "\n")

#--- imf_bop upload after fetch

def test_imf_bop_upload():
    test_message = "TRY UPLOAD AFTER FETCH"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    print("Data Glimpse:\n" + "-"*len("MData Glimpse:") + "\n")
    imf_bop = pysfo_pull.imfBOP.get(
        subdata = "Liabilities",
        INDICATOR = ["ILPD_BP6_USD"],
        FREQ = "Q"
    )
    print(imf_bop.tail(4))


#--- imf_bop check_reporting

def test_imf_bop_check_reporting():
    test_message = "TRY CHECK REPORTING OF SERIES"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    print("Data Glimpse:\n" + "-"*len("MData Glimpse:") + "\n")
    
    subdata = "Liabilities"
    INDICATOR = ["ILPD_BP6_USD"]
    FREQ = "Q"
    report = pysfo_pull.imfBOP.check_reporting(subdata, INDICATOR, FREQ)
    
    print(report.tail(4))
        