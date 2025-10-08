#%%========== packages ==========%%#

import pytest
import pysfo.pulldata as pysfo_pull

#%%========== tests ==========%%#

#--- imf_ifs dbDownload

def test_imf_ifs_dbDownload():
    test_message = "TRY IMF IFS DBNOMICS DONWLOAD"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")

    # test main series documentation
    main_series_docs = pysfo_pull.imf_ifs.dbDownload().main_series_documentation()[0:100]
    print("Main Series Documentation:\n" + "-"*len("Main Series Documentation:") + "\n" + main_series_docs + "\n")
    
    # test fetch series
    print("Test fetch series:\n" + "-"*len("Test fetch series:") + "\n")
    save_dir = "D:/Dropbox/80_data/raw/imf_ifs"
    subdata = "International Investment Positions"
    pysfo_pull.imf_ifs.dbDownload().fetch_and_save_series_by_subdata(subdata, save_dir)
    print("\n")

    # test subseries documentation
    subdata = "International Investment Positions"
    subdata_docs = pysfo_pull.imf_ifs.dbDownload().subseries_documentation(subdata)[0:200]
    print("Sub-Series Documentation:\n" + "-"*len("Sub-Series Documentation:") + "\n" + subdata_docs + "\n")

#--- imf_ifs upload after fetch

def test_imf_ifs_upload():
    test_message = "TRY UPLOAD AFTER FETCH"
    print("\n\n")
    print("_"*len(test_message))
    print(f"{test_message}\n\n")
    
    print("Data Glimpse:\n" + "-"*len("MData Glimpse:") + "\n")
    try:
        save_dir = "D:/Dropbox/80_data/raw/imf_ifs"
        subdata = "International Investment Positions"
        pysfo_pull.imf_ifs.dbDownload().fetch_and_save_series_by_subdata(subdata, save_dir)

        imf_ifs = pysfo_pull.imfIFS.get(
            subdata = "Gross_Domestic_Product",
            INDICATOR = ["NGDP_NSA_XDC",
                         "NGDP_R_NSA_XDC"],
            FREQ = "Q"
        )
        print(imf_ifs.tail(4))
        assert not imf_ifs.empty
    except FileNotFoundError:
        pytest.skip("IMF IFS data missing.")


