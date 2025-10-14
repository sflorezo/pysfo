#%%========== helper functions ==========%%#

def _decompose_indicator_df():

    import os
    from pysfo.pulldata.dbnomicstools.config import get_filters
    import re

    current_dir = os.path.dirname(__file__)
    file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
    json_metadata_path = os.path.join(current_dir, file)
    
    indicator_df = get_filters(json_metadata_path, filter = "indicator")

    DESC = indicator_df["DESCRIPTION_TEXT"].str.split(",", expand = True)
    DESC = DESC.apply(lambda col : col.str.strip(), axis = 1)

    indicator_df[[f"DESCRIPTION_TEXT_{i}" for i, _ in enumerate(DESC, start = 1)]] = DESC

    return indicator_df

def _fetch_and_save_series_all_ctys(indicator_list, save_dir, force_fetch = False):

    import pandas as pd
    import dbnomics as db
    import os
    from joblib import Parallel, delayed
    from functools import partial
    from pysfo.pulldata.dbnomicstools.config import get_filters
    from pysfo.basic import flatten_list
    import re

    # set arguments

    indicator_list = [indicator_list] if isinstance(indicator_list, str) else indicator_list
    
    #---- helper functions

    def chunk_series_to_fetch_list(list_of_series_to_fetch, n):

        chunked = []
        for i in range(0, len(list_of_series_to_fetch), n):
            chunked.append(list_of_series_to_fetch[i:i+n])

        return chunked

    def _fetch_wb_wdi(series_list):
        
        fetched_list = []
        for series in series_list:

            df = db.fetch_series(
                'WB', 'WDI',
                series
            )

            fetched_list.append({series : df})

        fetched_list = {k: v for d in fetched_list for k, v in d.items()}
            
        return fetched_list

    #---- fetch series main code

    current_dir = os.path.dirname(__file__)
    file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
    json_metadata_path = os.path.join(current_dir, file)

    frequency_df = get_filters(json_metadata_path, filter = "frequency")
    ref_area_df = get_filters(json_metadata_path, filter = "country")
    
    fetch_frequencies = frequency_df.loc[:, "VALUE"].to_list()
    fetch_ref_areas = ref_area_df.loc[:, "VALUE"].to_list()

    check_existence = [
        os.path.exists(f"{save_dir}/{indicator}.csv") and not force_fetch for indicator in indicator_list
    ]

    for exists, _ind in zip(check_existence, indicator_list):
        if exists:
            print(f"\nData for {_ind} already fetched. Removing from final fetch list. Please add flag force_fetch = True if you want new fetch and overwrite current file.")
            indicator_list = [ind for ind in indicator_list if ind != _ind]

    if len(indicator_list) == 0:
        print(f"\nNo new data to fetch. Exiting.")
        return

    list_of_series_to_fetch = flatten_list([
        [
            [
                freq + "-" + ind + "-" + cty
                for freq in fetch_frequencies
            ] 
            for cty in fetch_ref_areas
        ]
        for ind in indicator_list
    ])

    fetch_msg = ", ".join(indicator_list)
    print(f"\nFetching series '{fetch_msg}'")

    max_nb_series_fetched = (
        len(list_of_series_to_fetch)
    )

    print(f". Max number of series fetched: {max_nb_series_fetched}")

    try :

        n = int(len(list_of_series_to_fetch) / 100)
        batches = chunk_series_to_fetch_list(list_of_series_to_fetch, n)

        results = Parallel(n_jobs = -1, verbose=10)(
            delayed(_fetch_wb_wdi)(batch) 
            for batch in batches
        )

        final_dict = {k: v for d in results for k, v in d.items()}

        data_by_series = {
            ind : {
                k: v for k, v in final_dict.items() if re.search(ind, k)
            }
            for ind in indicator_list
        }

        for series, data_dict in data_by_series.items():

            data_df_list = [df.astype(str) for df in data_dict.values()]
            data_df = pd.concat(data_df_list, axis = 0)

            data_df = data_df.to_csv(f"{save_dir}/{series}.csv")
    
    except Exception as e:
        
        raise ValueError(f"Error importing series {indicator_list}")

#%%========== fetch data ==========%%#

import os

class dbDownload:

    def __init__(self):

        import os
        from pathlib import Path
        
        self._base_dir = Path(os.getcwd())
        self._indicator_df = _decompose_indicator_df()

    def main_series_documentation(self, store_docs=False):

        import io

        # Create an in-memory text buffer
        buffer = io.StringIO()

        for _, row in self._indicator_df.iterrows():
            
            series = row["VALUE"]
            desc = row["DESCRIPTION_TEXT"]
            buffer.write(f"{f'({series})':<30} {desc}\n")
        
        # Retrieve full captured string
        result_str = buffer.getvalue()
        buffer.close()

        if store_docs:
            docs_dir = os.path.join(self._base_dir, "docs")
            os.makedirs(docs_dir, exist_ok=True)

            output_path = os.path.join(docs_dir, "0_ALL_SERIES_DESC.txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result_str)

        return result_str
    
    def fetch_and_save_series_all_ctys(self, indicator_list, save_dir, force_fetch = False):
        
        _fetch_and_save_series_all_ctys(indicator_list, save_dir, force_fetch)
    
    def example_code(self):

        example_code = (

            """
            #========== packages and paths ==========#

            import pysfo.pulldata as pysfo_pull
            from pysfo.basic.basicfns import *
            import os
            from dotenv import load_dotenv

            load_dotenv("C:/Users/saflo/.env")
            pysfo_pull.set_data_path(os.getenv("MASTER_RAW_PATH"))
            wdi_dbDownload = pysfo_pull.wbWDI.dbDownload()

            #========== fetch data ==========#

            # get main subdata descriptions

            _ = wdi_dbDownload.main_series_documentation(store_docs = True)

            # fetch interest subdatasets, and get documentation

            fetch_indicator_list = [
                "NY.GDP.MKTP.CD",
                "NY.GDP.MKTP.KD",
                "NY.GDP.MKTP.KD.ZG"
            ]

            wdi_dbDownload.fetch_and_save_series_all_ctys(fetch_indicator_list, save_dir = os.path.dirname(__file__))

            """
        )

        return example_code
