#%%========== packages ==========%%#

import pytest
import pysfo.pulldata as pysfo_pull

#%%========== tests ==========%%#

#--- imf_weo about and documentation

# def test_imf_weo_documentation():
#     test_message = "TRY IMF IFS DOCUMENTATION"
#     print("\n\n")
#     print("_"*50)
#     print(f"{test_message}\n\n")

#     about = pysfo_pull.imfWEO.about()
#     print("About:\n" + "-"*len("About:") + "\n" + about + "\n")

#     instructions = pysfo_pull.imfWEO.print_instructions()
#     print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")

#--- imf_weo dbDownload

def test_imf_weo_dbDownload():
    test_message = "TRY IMF IFS DBNOMICS DONWLOAD"
    print("\n\n")
    print("_"*50)
    print(f"{test_message}\n\n")

    # test fetch series
    print("Test fetch series:\n" + "-"*len("Test fetch series:") + "\n")
    save_dir = "D:/Dropbox/80_data/raw/imf_weo"
    pysfo_pull.imfWEO.dbDownload().fetch_and_save_weo(save_dir)
    print("fetched succesfully")

    # test example code
    example_code = pysfo_pull.imfWEO.dbDownload().example_code()
    print("Example code:\n" + "-"*len("Example code:") + "\n" + example_code + "\n")
    
# #--- imf_weo upload after fetch

# def test_imf_weo_upload():
#     test_message = "TRY UPLOAD AFTER FETCH"
#     print("\n\n")
#     print("_"*len(test_message))
#     print(f"{test_message}\n\n")
    
#     print("Data Glimpse:\n" + "-"*len("MData Glimpse:") + "\n")
#     try:
#         save_dir = "D:/Dropbox/80_data/raw/imf_weo"
#         subdata = "International Investment Positions"
#         pysfo_pull.imfWEO.dbDownload().fetch_and_save_series_by_subdata(subdata, save_dir)

#         imf_weo = pysfo_pull.imfWEO.get(
#             subdata = "Gross_Domestic_Product",
#             INDICATOR = ["NGDP_NSA_XDC",
#                          "NGDP_R_NSA_XDC"],
#             FREQ = "Q"
#         )
#         print(imf_weo.tail(4))
#         assert not imf_weo.empty
#     except FileNotFoundError:
#         pytest.skip("IMF IFS data missing.")