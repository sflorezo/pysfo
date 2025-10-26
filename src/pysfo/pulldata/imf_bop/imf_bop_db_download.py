#%%========== helper functions ==========%%#

def _decompose_indicator_df():

    import os
    from pysfo.pulldata.dbnomicstools.config import get_filters
    import re

    current_dir = os.path.dirname(__file__)
    file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
    json_metadata_path = os.path.join(current_dir, file)
    
    indicator_df = get_filters(json_metadata_path, filter = "INDICATOR")

    DESC = indicator_df["DESCRIPTION_TEXT"].str.split(",", expand = True)
    DESC = DESC.apply(lambda col : col.str.strip(), axis = 1)

    indicator_df[[f"DESCRIPTION_TEXT_{i}" for i, _ in enumerate(DESC, start = 1)]] = DESC

    return indicator_df

def _fetch_and_save_series_by_subdata(subdata, save_dir, force_fetch = False):

    import pandas as pd
    import dbnomics as db
    import os
    from joblib import Parallel, delayed
    from functools import partial
    from pysfo.pulldata.dbnomicstools.config import get_filters
    import re

    # import pysfo.pulldata as pysfo_pulldata
    # pysfo_pulldata.set_data_path("D:/Dropbox/80_data/raw")
    # subdata = "Assets"
    # save_dir = pysfo_pulldata.get_data_path() / "imf_bop"
    
    #---- helper functions

    def chunk_dimensions_dict(mydict, n):

        not_series_dimensions = {key : val for key, val in mydict.items() if key != "INDICATOR"}

        series = mydict["INDICATOR"]

        chunked = []
        for i in range(0, len(series), n):
            chunked.append(series[i:i+n])

        chunked = [{"INDICATOR" : chunk} for chunk in chunked]
        if len(not_series_dimensions) > 0 :
            chunked = [
                {**chunk, **not_series_dimensions}
            for chunk in chunked]

        return chunked

    fetch_partial = partial(
        db.fetch_series,
        provider_code = "IMF",
        dataset_code = "BOP"
    )

    #---- fetch series main code

    subdata_list = [subdata] if isinstance(subdata, str) else subdata

    current_dir = os.path.dirname(__file__)
    file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
    json_metadata_path = os.path.join(current_dir, file)

    frequency_df = get_filters(json_metadata_path, filter = "FREQ")
    ref_area_df = get_filters(json_metadata_path, filter = "REF_AREA")
    indicator_df = _decompose_indicator_df()

    subdata_list_all = indicator_df["DESCRIPTION_TEXT_1"].unique()
    check = (
        True in [
            to_fetch not in subdata_list_all for
        to_fetch in subdata_list]
    )

    if check:
        raise ValueError("'subdata' not found in the list of available sub-datasets. Please check main series documentation to see what can be retrieved.")

    for subdata_ in subdata_list:

        dataname = subdata_.replace(" ", "_")

        if os.path.exists(f"{save_dir}/{dataname}.csv") and not force_fetch:
            
            print(f"\nData for {dataname} already fetched. Please add flag force_fetch = True if you want new fetch and overwrite current file.")

        else:
            
            print(f"\nFetching series for '{subdata_}'")

            mask = (
                indicator_df["DESCRIPTION_TEXT_1"]
                .str.contains(
                    subdata_, 
                    regex = False,
                    case = False
                )
            )

            fetch_indicators =  indicator_df.loc[mask, "VALUE"].to_list()
            fetch_frequencies = frequency_df.loc[:, "VALUE"].to_list()
            fetch_ref_areas = ref_area_df.loc[:, "VALUE"].to_list()
            
            get_dimensions = {
                "INDICATOR" : fetch_indicators,
            }

            max_nb_series_fetched = (
                len(fetch_ref_areas) 
                * len(fetch_frequencies) 
                * len(fetch_indicators)
            )

            try :
                
                if max_nb_series_fetched <= 40000:
                    
                    df = db.fetch_series(
                        'IMF', 'IFS',
                        dimensions = get_dimensions,
                        max_nb_series = (
                            len(fetch_ref_areas) 
                            * len(fetch_frequencies) 
                            * len(fetch_indicators)
                        ),
                        timeout = 60
                    )

                else :
                    
                    # recall n is size of each individual batch.

                    if subdata_ == "Assets":
                        n = 1
                    else:
                        n = 1 # int(len(fetch_indicators) / 10)
                    dimension_batches = chunk_dimensions_dict(get_dimensions, n)
                    max_nb_series_batches = [
                        len(batch["INDICATOR"]) 
                        * len(fetch_frequencies) 
                        * len(fetch_indicators)
                        for batch in dimension_batches
                    ]

                    batch_job_size = len(dimension_batches)

                    print(f". Running batch job with: {batch_job_size}")

                    results = Parallel(n_jobs = -1, verbose=10)(
                        delayed(fetch_partial)(
                            dimensions = dim_batch,
                            max_nb_series = max_nb_s
                        ) 
                        for dim_batch, max_nb_s in zip(dimension_batches, max_nb_series_batches)
                    )

                    df = pd.concat(results, axis = 0)
                
            except Exception as e:
                
                df = pd.DataFrame([e])
                dataname = dataname + "_ERROR"

            df.to_csv(f"{save_dir}/{dataname}.csv")

def _subdata_documentation_fullstr(subdata, base_dir):

    import pandas as pd
    import io

    #---- helper functions
    
    def _rename_dataset(df):
        return df.rename(columns={"Indicator": "indicator_label"})

    #---- main

    df = pd.read_csv(f"{base_dir}/{subdata}.csv", dtype=str)
    df = _rename_dataset(df)
    df.columns = df.columns.str.lower()

    IND_CODE = pd.DataFrame(df["indicator"])
    IND_LAB_FULL = pd.DataFrame(df["indicator_label"])
    FREQ = pd.DataFrame(df["freq"])

    IND_LABS = df["indicator_label"].str.split(",", expand=True).apply(lambda col: col.str.strip())
    IND_LABS.columns = [f"indicator_label_{i}" for i, _ in enumerate(IND_LABS, start=1)]

    IND = pd.concat([IND_CODE, FREQ, IND_LAB_FULL, IND_LABS], axis=1)
    IND = IND.drop_duplicates()
    split_labels = IND.filter(regex = r"indicator_label_\d+").columns.to_list()
    IND = IND.sort_values(by = split_labels).reset_index(drop=True)
    IND["indicator_label_short"] = IND[split_labels[2:]].apply(lambda x : ", ".join(x.fillna("")), axis=1)
    IND["indicator_label_short"] = (
        IND["indicator_label_short"]
        .str.replace(r"(,\s*)+$", "", regex=True)
        .str.strip()
    )

    buffer = io.StringIO()

    for lab2 in IND["indicator_label_2"].unique():
        buffer.write(f"\n#========== {lab2} ==========#\n\n")
        mask = IND["indicator_label_2"] == lab2
        inds = IND.loc[mask, "indicator"]
        freqs = IND.loc[mask, "freq"]
        inds_labs = IND.loc[mask, "indicator_label_short"]

        for ind, freq, main_lab in zip(inds, freqs, inds_labs):
            ind = f"({freq}, {ind})"
            buffer.write(f"{ind:<30}{main_lab}\n")

    output_str = buffer.getvalue()

    return output_str

#%%========== fetch data ==========%%#

import os

class dbDownload:

    def __init__(self):

        # import pysfo.pulldata as pysfo_pulldata
        # pysfo_pulldata.set_data_path("D:/Dropbox/80_Data/raw")

        from pysfo.pulldata import get_data_path
        
        self._base_dir = get_data_path() / "imf_bop"
        self._indicator_df = _decompose_indicator_df()

    def get_indicator_decomposed(self, subdata):
        
        indicator_df = self._indicator_df
        indicator_df = indicator_df[
            indicator_df["DESCRIPTION_TEXT_1"] == subdata
        ]

        return indicator_df

    def main_series_documentation(self, store_docs = False):
        
        lines = []
        for s1 in self._indicator_df["DESCRIPTION_TEXT_1"].unique():
            lines.append(f"\n#========== subdata: '{s1}' ==========#\n")
            mask1 = self._indicator_df["DESCRIPTION_TEXT_1"] == s1
            hdf1 = self._indicator_df.loc[mask1, :]
            for s2 in hdf1["DESCRIPTION_TEXT_2"].unique():
                lines.append(f"{s2}")
        result_str = "\n".join(lines)
        
        if store_docs:
            docs_dir = os.path.join(self._base_dir, "docs")

            if os.path.exists(docs_dir):
                pass
            else:
                os.makedirs(docs_dir, exist_ok=True)

            output_path = os.path.join(docs_dir, f"0_ALL_SERIES_DESC.txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result_str)

        return result_str

    def subseries_documentation(self, subdata, store_docs = False):

        subdata = subdata.replace(" ", "_")
        
        documentation_string = _subdata_documentation_fullstr(subdata, self._base_dir)

        if store_docs:
            
            docs_dir = os.path.join(self._base_dir, "docs")

            if os.path.exists(docs_dir):
                pass
            else:
                os.makedirs(docs_dir, exist_ok=True)

            output_path = os.path.join(docs_dir, f"{subdata}.txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(documentation_string)

                print(f"Saved formatted indicators to: {output_path}")

        return documentation_string

    def fetch_and_save_series_by_subdata(self, subdata, save_dir, force_fetch = False):
        
        _fetch_and_save_series_by_subdata(subdata, save_dir, force_fetch)
    
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
            bop_dbDownload = pysfo_pull.imfBOP.dbDownload()


            #========== fetch data ==========#

            # get main subdata descriptions

            _ = bop_dbDownload.main_series_documentation(store_docs = True)

            # # fetch interest subdatasets, and get documentation

            fetch_subdata_list = [
                "Supplementary Items",
                "Exceptional Financing",
                "Financial Account",
                "Assets",
                "Assets (with Fund Record)",
                "Liabilities"
            ]

            for subdata in fetch_subdata_list:

                bop_dbDownload.fetch_and_save_series_by_subdata(subdata, save_dir = os.path.dirname(__file__))
                _ = bop_dbDownload.subseries_documentation(subdata, store_docs = True)
            """
        )

        return example_code
