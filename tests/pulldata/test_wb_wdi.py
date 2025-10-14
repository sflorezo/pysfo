#%%========== packages ==========%%#

import pytest
import pysfo.pulldata as pysfo_pull

# pysfo_pull.set_data_path("D:/Dropbox/80_data/raw")

#%%========== tests ==========%%#

#--- imf_ifs about and documentation

def test_wb_wdi_documentation():
    test_message = "TRY WB WDI DOCUMENTATION"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    about = pysfo_pull.wbWDI.about()
    print("About:\n" + "-"*len("About:") + "\n" + about + "\n")

    instructions = pysfo_pull.wbWDI.print_instructions()
    print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")

#--- wb_wdi dbDownload

def test_wb_wdi_dbDownload():
    test_message = "TRY WB WDI DBNOMICS DONWLOAD"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    # test main series documentation
    main_series_docs = pysfo_pull.wbWDI.dbDownload().main_series_documentation()[0:100]
    print("Main Series Documentation:\n" + "-"*len("Main Series Documentation:") + "\n" + main_series_docs + "\n")
    
    # test fetch series
    print("Test fetch series:\n" + "-"*len("Test fetch series:") + "\n")
    save_dir = "D:/Dropbox/80_data/raw/wb_wdi"
    indicator_list = "NY.GDP.MKTP.CD"
    pysfo_pull.wbWDI.dbDownload().fetch_and_save_series_all_ctys(indicator_list, save_dir)
    print("\n")

    # test example code
    example_code = pysfo_pull.wbWDI.dbDownload().example_code()
    print("Example code:\n" + "-"*len("Example code:") + "\n" + example_code + "\n")

#--- imf_ifs upload after fetch

def test_imf_ifs_upload():
    test_message = "TRY UPLOAD AFTER FETCH"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    print("Data Glimpse:\n" + "-"*len("MData Glimpse:") + "\n")
    try:
        
        indicator = "NY.GDP.MKTP.CD"
        wb_wdi = pysfo_pull.wbWDI.get(indicator)
        print(wb_wdi.tail(4))
        assert not wb_wdi.empty
    except FileNotFoundError:
        pytest.skip("WB WDI data missing.")


