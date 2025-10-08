#%%========== helper functions ==========%%#
    
def _decompose_indicator_df():

    import pysfo.dbnomicstools.imf_ifs as dbt_ifs
    
    indicator_df = dbt_ifs.get_filters(filter = "INDICATOR")

    DESC = indicator_df["DESCRIPTION_TEXT"].str.split(",", expand = True)
    DESC = DESC.apply(lambda col : col.str.strip(), axis = 1)

    indicator_df[[f"DESCRIPTION_TEXT_{i}" for i, _ in enumerate(DESC, start = 1)]] = DESC

    return indicator_df

def _fetch_and_save_series_by_subdata(subdata, save_dir, force_fetch = False):

    import pandas as pd
    import dbnomics as db
    import os
    import pysfo.dbnomicstools.imf_ifs as dbt_ifs
    from joblib import Parallel, delayed
    from functools import partial
    
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

    fetch_imf_ifs = partial(
        db.fetch_series,
        provider_code = "IMF",
        dataset_code = "IFS"
    )

    #---- fetch series main code

    subdata_list = [subdata] if isinstance(subdata, str) else subdata

    frequency_df = dbt_ifs.get_filters(filter = "FREQ")
    ref_area_df = dbt_ifs.get_filters(filter = "REF_AREA")
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
                "INDICATOR": fetch_indicators,
            }

            max_nb_series_fetched = (
                len(fetch_ref_areas) 
                * len(fetch_frequencies) 
                * len(fetch_indicators)
            )

            print(f". Max number of series fetched: {max_nb_series_fetched}")

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
                    
                    if subdata_ != "Exchange Rates":
                        n = int(len(fetch_ref_areas) / 10)
                    else:
                        n = int(len(fetch_indicators) / 10)
                    dimension_batches = chunk_dimensions_dict(get_dimensions, n)
                    max_nb_series_batches = [
                        len(batch["INDICATOR"]) 
                        * len(fetch_frequencies) 
                        * len(fetch_indicators)
                        for batch in dimension_batches
                    ]

                    results = Parallel(n_jobs = -1, verbose=10)(
                        delayed(fetch_imf_ifs)(
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

        from ..config import get_data_path
        
        self.base_dir = get_data_path() / "imf_ifs"
        self.indicator_df = _decompose_indicator_df()

    def main_series_documentation(self, store_docs = False):
        
        lines = []
        for s1 in self.indicator_df["DESCRIPTION_TEXT_1"].unique():
            lines.append(f"\n#========== subdata: '{s1}' ==========#\n")
            mask1 = self.indicator_df["DESCRIPTION_TEXT_1"] == s1
            hdf1 = self.indicator_df.loc[mask1, :]
            for s2 in hdf1["DESCRIPTION_TEXT_2"].unique():
                lines.append(f"{s2}")
        result_str = "\n".join(lines)
        
        if store_docs:
            docs_dir = os.path.join(self.base_dir, "docs")

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
        
        documentation_string = _subdata_documentation_fullstr(subdata, self.base_dir)

        if store_docs:
            
            docs_dir = os.path.join(self.base_dir, "docs")

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