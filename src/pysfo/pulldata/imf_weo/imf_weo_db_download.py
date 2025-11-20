#%%========== helper functions ==========%%#

def _fetch_and_save_weo(save_dir, force_fetch = False):

    import dbnomics as db
    import numpy as np
    import os
    from pysfo.pulldata.dbnomicstools.config import get_filters
    from pathlib import Path

    current_dir = os.path.dirname(__file__)
    json_path = os.path.join(current_dir, "imf_weo_db_datastructure.json")

    all_filters = get_filters(json_metadata_path = json_path)[["ID", "NAME", "VALUE"]]
    
    filters_dict = {}
    for filter_ in all_filters["ID"].unique():
        mask = (all_filters["ID"] == filter_)
        filters_dict[filter_] = all_filters.loc[mask, "VALUE"]
    
    max_nb_series = int(
        np.prod([
        len(filters_dict[filter_]) 
        for filter_ in filters_dict.keys()])
    )

    # check if file exists

    file_path = Path(save_dir) / "WEO_oct2024.csv"

    if file_path.exists() and not force_fetch:
        print(f"File {file_path} already exists. Skipping fetch. To force fetch, set force_fetch = True")
        return 

    df = db.fetch_series(
        'IMF', 'WEOAGG:2024-10',
        max_nb_series = max_nb_series,
        timeout = 60
    )

    df.to_csv(f"{file_path}")

    return f"WEO data saved in File '{file_path}'"

#%%========== fetch data ==========%%#

class dbDownload:

    def __init__(self):

        from ..config import get_data_path
        
        self._base_dir = get_data_path() / "imf_weo"

    def fetch_and_save_weo(self, save_dir):
        
        _fetch_and_save_weo(save_dir)
    
    def example_code(self):

        example_code = (

            """
            #========== packages and paths ==========#

            To Write.

            """
        )
        
        return example_code
