
#%% 

import pandas as pd
import dbnomics as db
import os
from joblib import Parallel, delayed
from pysfo.pulldata.dbnomicstools.config import get_filters
from pysfo.basic import silent_call, save_parquet
from pysfo.pulldata import set_data_path, get_data_path
from typing import Dict, Any, cast
import re
from pathlib import Path

# from pysfo.basic import *
# from pysfo.pulldata import set_data_path
# set_data_path("/storage/Dropbox/80_data/raw")
# idx_with_data = [4100:4110]

#%% ========== script-specific params ========== %%#

non_fetched_error_file = "bis_dss_download_non_fetched_error_file_REF_AREA_{REF_AREA}.txt"

current_dir = os.path.dirname(__file__)
file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
json_metadata_path = os.path.join(current_dir, file)

#%% ========== helper functions ========== %%#


def _fetch_batch(batch_dimensions : dict) -> Dict[str, Any]:

    #######
    # batch_dimensions = download_dimensions_batches[3190:3215][0]
    #######

    not_fetched_error = None
    df = None

    try :

        df = silent_call(
            db.fetch_series, 
            provider_code = 'BIS', 
            dataset_code = 'WS_NA_SEC_DSS',
            dimensions = batch_dimensions,
            max_nb_series = 5000,
            timeout = 60,
            verbose = False
        )

    except db.FetchError as e:
        
        not_fetched_error = f"{e}"
    
    return {
        "dimensions" : batch_dimensions,
        "fetched_df" : df, 
        "not_fetched_error": not_fetched_error
    }

def _fetch_and_save_bisDSS_by_ref_area(
        root_raw_bis_dss_path : Path,
        ref_area : str,
        csv_save_dir : Path, 
        force_fetch = False
):
    
    from .params import bis_dss_fetch_root_name

    ########
    # ref_area = "US"
    # force_fetch = False
    # csv_save_dir = Path("/storage/Dropbox/80_data/raw/bis_dss/REF_AREA_US.csv")
    ########

    #---- check everything looks good

    csv_save_file = csv_save_dir / (bis_dss_fetch_root_name.format(REF_AREA = ref_area) + ".csv")

    if csv_save_dir.is_dir() is False:

        raise ValueError("csv_save_dir must be a directory path. (where the raw data will be stored)")

    if csv_save_file.exists():
    
        if force_fetch == False:

            _msg = (
                f"File already exists at {csv_save_file}.\n"
                "Skipping file and continuing with other non-downloaded files (fix force_fetch = True to overwrite)."
            )
            print(_msg)
            return
        
        else:

            _msg = (
                f"File already exists at {csv_save_file}.\n"
                "Overwriting file (fix force_fetch = False to skip)."
            )

            print(_msg)

    else:
        
        _msg = (
            f"File does not exist for REF_AREA = {ref_area}. Fetching data."
        )
        print(_msg)

    #---- get BIS DSS available dimensions

    ref_area_df = get_filters(json_metadata_path, filter = "REF_AREA")
    ref_sector_df = get_filters(json_metadata_path, filter = "REF_SECTOR")

    #---- check ref_area is in available BIS DSS data

    if ref_area not in ref_area_df["VALUE"].values:
        _msg = (
            f"{ref_area} not in available BIS DSS data.\n"
            # "Please check available values using get_available_ref_area()"
        )
        raise ValueError(_msg)

    #---- fetch series main code

    fetch_ref_sector = cast(list, ref_sector_df.loc[:, "VALUE"].to_list())
    
    download_dimensions_batches = [
        {
            "REF_AREA" : [ref_area],
            "REF_SECTOR" : [ref_sector]
        }
        for ref_sector in fetch_ref_sector
    ]
    
    results = Parallel(n_jobs = -1, verbose=10)(
        delayed(_fetch_batch)(
            batch_dimensions
        ) 
        for batch_dimensions in download_dimensions_batches
    )

    results = cast(list, results)

    #---- handle errors

    fetch_errors = [
        res["dimensions"]
        for res in results
        if res["not_fetched_error"] is not None
    ]

    if len(fetch_errors) == 0:
        print(f"All series fetched successfully")
    else :
        
        _file = root_raw_bis_dss_path / Path(str(non_fetched_error_file).format(REF_AREA = ref_area))

        _msg = (
            "Not all series fetched succesfully.\n" \
            f"Storing non-fetched series errors in {_file}"
        )
        print(_msg)
        
        fetch_errors = "\n".join([str(el) for el in fetch_errors])
        _file.write_text(cast(str, fetch_errors))

    #---- handle dataframes

    result_df_list = [res["fetched_df"] for res in results if not res["fetched_df"].empty] 
    
    if len(result_df_list) > 0:
        df = cast(pd.DataFrame, pd.concat(result_df_list, axis = 0))
        df.to_csv(csv_save_file)
        print(f"Data saved in {csv_save_file}")

    else :
        print("No data fetched")

#%% ========== callable functions ========== %%#


class dbDownload:

    def __init__(self):

        from pysfo.pulldata import get_data_path
        
        self._base_dir = get_data_path() / "bis_dss"

    def fetch_and_save_bisDSS_by_ref_area(self, ref_area, csv_save_dir, force_fetch = False):
        
        root_raw_bis_dss_path = self._base_dir

        _fetch_and_save_bisDSS_by_ref_area(
            root_raw_bis_dss_path,
            ref_area,
            csv_save_dir, 
            force_fetch
        )
    
    def example_code(self):

        example_code = (

            """
            #========== packages and paths ==========#

            To Write.

            """
        )
        
        return example_code
    