#%%========== Reporting Handler ==========%%#

class checkReporting:

    def __init__(
            self,
            provider,
            dataset,
            subdata, 
            series, 
            freq, 
            summarized,
            report_percen, 
            start_date, 
            end_date,
            REF_AREA_all
        ):

        from pysfo.pulldata.dbnomicstools.check_timeseries_reporting.get_dataset_keys import get_dataset_keys
        
        if summarized and report_percen != 1:
            print("Setting report_percen = 1 for summary of report as summarized = 1.")

        (
            self.series_key, 
            self.period_key, 
            self.country_key, 
            self.country_key_iso, 
            self.country_key_desc, 
            self.freq_key, 
            self.metadata_path, 
            self.ignore_countries,
            self.get_series_fn
        ) = get_dataset_keys(provider, dataset)

        self.subdata = subdata
        self.series = series
        self.freq = freq
        self.summarized = summarized
        self.report_percen = report_percen if not summarized else 1
        self.start_date = start_date
        self.end_date = end_date
        self.REF_AREA_all = REF_AREA_all

    def _generate_gdp_skeleton(self, all_periods):

        import pysfo.pulldata as pysfo_pull
        import os
        import re
        import pandas as pd
        import textwrap
        import numpy as np

        #  self = checkReporting(provider, dataset, subdata, series, freq, summarized, report_percen, start_date, end_date, REF_AREA_all)

        file = [file for file in os.listdir(self.metadata_path) if re.findall(".customization", file)][0]
        json_metadata_path = os.path.join(self.metadata_path, file)

        all_country_keys = pysfo_pull.dbnomicstools.get_filters(json_metadata_path, filter = "REF_AREA")

        all_country_keys = all_country_keys[["VALUE", "DESCRIPTION_TEXT"]]
        all_country_keys.columns = [self.country_key, self.country_key_desc]

        skeleton = (
            pd.MultiIndex.from_product(
                [all_country_keys[self.country_key], all_periods],
                names=[self.country_key, self.period_key]
            )
            .to_frame(index=False)
            .merge(all_country_keys, on=self.country_key, how="left")
            .sort_values([self.country_key, self.period_key])
            .reset_index(drop=True)
        )
        skeleton = skeleton[[self.country_key, self.country_key_desc, self.period_key]]

        # merge size

        try :
            size_orderer = pysfo_pull.wbWDI.get(indicator = "NY.GDP.MKTP.CD", silent = True)
        except FileNotFoundError as e:
            _message = textwrap.dedent("""\
            Need to pull indicator 'NY.GDP.MKTP.CD' from wbWDI before creating counrty report check. 
            For more info, please see the documentation on wbWDI
                                    
            instructions = pysfo_pull.wbWDI.print_instructions()
            print(instructions)
            """)
            raise ValueError(_message)

        size_orderer = size_orderer[
            (size_orderer["cty_iso2"] != "not found")
            | (size_orderer["cty_iso2"].isna())
        ]

        # size_orderer_merge_checker = (
        #     size_orderer[["cty_iso2", "cty_name"]]
        #     .drop_duplicates()
        # )

        size_orderer = (
            size_orderer
            .dropna(subset = ["value"])
            .sort_values(["cty_iso2", self.period_key])
            .groupby("cty_iso2", as_index=False)
            .tail(1)[["cty_iso2", self.period_key, "value"]]
            .reset_index(drop = True)
            .rename(columns = {"cty_iso2" : self.country_key,
                            self.period_key : "gdp_last_available_period",
                            "value" : "last_gdp"})
        )

        size_orderer = all_country_keys.merge(
            size_orderer[[self.country_key, "gdp_last_available_period", "last_gdp"]], 
            on = self.country_key,
            how = "outer",
            indicator = True,
            validate = "1:1"
        )
        
        see_left_only = size_orderer.loc[size_orderer["_merge"] == "left_only", self.country_key]
        mask = all_country_keys[self.country_key].isin(see_left_only)
        check = all_country_keys.loc[mask, self.country_key_desc]

        for ig in self.ignore_countries:

            check = check[~(check.str.match(ig, case = False))]

        if len(check):
            missing = "\n".join(check)
            _message = textwrap.dedent(f"""\
            Error constructing reporter_check for {self.series} ({self.freq}). Please check construction.
            Following countries still not matched:
            {missing}
            """)

            raise ValueError(_message)
        
        size_orderer["gdp_available"] = np.where(size_orderer["_merge"] == "left_only", 0, 1)
        size_orderer.drop(columns = "_merge", inplace = True)
        
        skeleton_w_gdp = (
            pd.MultiIndex.from_product(
                [size_orderer[self.country_key], all_periods],
                names=[self.country_key, self.period_key]
            )
            .to_frame(index=False)
            .merge(size_orderer, on=self.country_key, how="left")
            .sort_values([self.country_key, self.period_key])
            .reset_index(drop=True)
        )
        
        skeleton_w_gdp["last_gdp"] = skeleton_w_gdp["last_gdp"].fillna(0)

        return skeleton_w_gdp
    
    def _check_reporting_for_individual_indicator(self, df_series, skeleton_w_gdp):

        import pandas as pd
        from pysfo.basic import statatab, silent_call
        import numpy as np
        import textwrap
        import country_converter as coco

        cc = coco.CountryConverter()

        #  self = checkReporting(provider, dataset, subdata, series, freq, summarized, report_percen, start_date, end_date, REF_AREA_all)
        # df_series = df[df[self.series_key] == df[self.series_key].unique()[0]]

        SERIES = df_series[self.series_key].unique()

        if len(SERIES) > 1:
            raise ValueError(f"df_series should only contain one series. Please check construction.")

        # merge df_series and skeleton_w_gdp

        try:
            h_df = df_series.merge(skeleton_w_gdp, how = "outer", on = [self.period_key, self.country_key], indicator = True, validate = "1:1")
        except pd.errors.MergeError as e:
            raise pd.errors.MergeError(
                f"MergeError in subdata={self.subdata}, {self.series_key}={SERIES}, {self.freq_key}={self.freq}: \n {e}"
            ) from e

        _merge_results = statatab(h_df["_merge"])
        if "left_only" in [col["Value"] for _, col in _merge_results.iterrows() if col["Count"] != 0]:
            raise ValueError(f"Error constructing reporter_check for {SERIES} ({self.freq}). Please check construction.")
        
        h_df["_merge"] = np.where(h_df["_merge"] == "both", 1, 0)
        h_df["decade"] = (h_df[self.period_key].dt.year // 10) * 10
        h_df = h_df.sort_values(by = [self.country_key, self.period_key])
        
        if self.freq == "A":
            to_year = 1
        elif self.freq == "Q":
            to_year = 4
        elif self.freq == "M":
            to_year = 12

        # check country ids (from full-sized skeleton) are non-missing before collapsing

        if True in ((h_df[self.country_key].isna()) | (h_df[self.period_key].isna())):
            raise ValueError(f"Error constructing reporter_check for {SERIES} ({self.freq}). Please check construction.")
        
        # collapse at decade and find number of years reporting

        not_to_pivot = ["gdp_last_available_period", "last_gdp", "gdp_available"]
        meta = h_df[[self.country_key, self.country_key_desc] + not_to_pivot].drop_duplicates()
        
        h_df = (
            h_df
            .groupby([self.country_key, self.country_key_desc, "decade"])
            .agg(
                merged_years = ("_merge", lambda x : x.sum() / to_year),
                gdp_last_available_period = ("gdp_last_available_period", "first"),
                last_gdp = ("last_gdp", "first"),
            )
            .reset_index()
            .pivot_table(
                index = [self.country_key, self.country_key_desc],
                columns = "decade",
                values = "merged_years",
            ).reset_index()
        )
        h_df = h_df.merge(meta, on = [self.country_key, self.country_key_desc], how = "outer", validate = "1:1", indicator = True)

        _merge_results = statatab(h_df["_merge"])
        h_df["drop_for_table"] = (h_df["_merge"] == "right_only").astype(int)
        h_df.drop(columns = "_merge", inplace = True)
        
        # _checker = (
        #     _merge_results
        #     .loc[_merge_results["Value"] == "both", "Percentage"]
        #     .loc[0]
        # )
        # if _checker != 100:
        #     raise ValueError(f"Error constructing reporter_check for {SERIES} ({self.freq}). Please check construction.")

        decadevars = h_df.filter(regex = r"^\d{4}$").columns
        h_df = h_df[list(h_df.drop(columns = decadevars).columns) + list(decadevars)]

        h_df[decadevars] = h_df[decadevars].apply(lambda x : x.fillna(0))
        h_df[decadevars] = h_df[decadevars].round(2)

        h_df["perc_reported"] = h_df[decadevars].sum(axis=1) / h_df[decadevars].sum(axis=1).max()
        h_df = pd.concat([
            h_df.drop(columns = decadevars),
            h_df[decadevars]
        ], axis = 1)
        h_df["perc_reported"] = h_df["perc_reported"].round(2)

        if self.report_percen is not None:
            drop = h_df["perc_reported"] > self.report_percen
            h_df = h_df[~drop]

        # merge iso3 if available, and cty_name for missing if available

        all_country_keys = h_df[self.country_key].unique()

        to_iso = "ISO2" if self.country_key_iso == "ISO3" else "ISO3"
        to_iso_label = "cty_iso2" if self.country_key_iso == "ISO3" else "cty_iso3"

        add_iso = silent_call(cc.convert, all_country_keys, src = self.country_key_iso, to = to_iso, verbose = False) 
        add_iso = pd.DataFrame({self.country_key : all_country_keys, to_iso_label : add_iso})

        cty_name = silent_call(cc.convert, all_country_keys, src = self.country_key_iso, to = "name_short", verbose = False)
        cty_name = pd.DataFrame({self.country_key : all_country_keys, "cty_name" : cty_name})

        h_df = h_df.merge(add_iso, on = self.country_key, how = "left", validate = "1:1")
        h_df = h_df[["cty_iso3"] + list(h_df.drop(columns = "cty_iso3").columns)]
        h_df[self.country_key_desc] = np.where(h_df[self.country_key_desc].isna(), cty_name["cty_name"], h_df[self.country_key_desc])
        
        # final formats

        h_df = h_df.sort_values(by = "last_gdp", ascending = False).reset_index(drop = True)
        h_df.columns.name = None
        h_df["last_gdp"] = h_df["last_gdp"] / 1e9
        h_df["last_gdp"] = h_df["last_gdp"].round(2)
        h_df["gdp_last_available_period"] = h_df["gdp_last_available_period"].dt.year

        # rename and output
        
        h_df.rename(columns = {
            "gdp_last_available_period" : "GDP Last Reporting Year",
            "last_gdp" : "Last GDP Reported (USD Billion)",
            "perc_reported" : "Percentage of Periods Reported"
        }, inplace = True)

        # drop countries that don't report to the IMF, regardless of whether they have GDP available in WB-WDI
        # (At the end I decide not to use it bc there are interesting countries with GDP that don't report to the IMF, as the UAE)

        h_df.drop(columns = "drop_for_table", inplace = True)

        # if summarized = False, print final sample selection

        if not self.summarized:

            if self.REF_AREA_all == False:
                _message = textwrap.dedent("""\
                \n Reporting REF_AREA with GDP available in WB WDI. To see all possible 
                REF_AREA, please set flag REF_AREA_all = True.
                """)
                print(_message)
                h_df = h_df.loc[h_df["gdp_available"] == 1, :]

            h_df = h_df.drop(columns = "gdp_available")

            return h_df
        
        # if summarized = True, give sufficient stat on world reporting

        if self.summarized:
            
            size_orderer = skeleton_w_gdp.groupby(self.country_key).agg({"last_gdp" : "max"})

            total_last_gdp_bn = size_orderer["last_gdp"].sum() / 1e9

            avail_percen = h_df["Percentage of Periods Reported"]
            
            w = h_df["Last GDP Reported (USD Billion)"] / total_last_gdp_bn

            report_summary = {
                "mean_report" : float(np.dot(avail_percen, w)),
                "n_countries_summarized" : len(h_df)
            }
            
            return report_summary

    def create_report_tables(self):
        
        from pysfo.pulldata.imf_bop.master_upload import get
        from pysfo.basic import silent_call
        from pysfo.pulldata.exceptions import SeriesNotFoundError

        # self = checkReporting(provider, dataset, subdata, series, freq, summarized, report_percen, start_date, end_date, REF_AREA_all)

        series_list = [self.series] if isinstance(self.series, str) else self.series

        try :
            df = self.get_series_fn(self.subdata, series_list, self.freq, silent = True)
        except SeriesNotFoundError as e:
            if len(series_list) == 1:
                raise 
            else:
                print(f"{e.not_found_list} not found. Skipping...")
                not_found_list = e.not_found_list
                cleaned_indicators = [ind for ind in series_list if ind not in not_found_list]    
                df = get(self.subdata, cleaned_indicators, self.freq, silent = True)

        if self.start_date is not None:
            df = df[df[self.period_key] >= self.start_date]
        if self.end_date is not None:
            df = df[df[self.period_key] <= self.end_date]

        all_periods = sorted(df[self.period_key].unique())
        
        # call gdp skeleton

        skeleton_w_gdp = self._generate_gdp_skeleton(all_periods)
        
        # generate reports for each indicator

        verbose = True if df[self.series_key].nunique() == 1 else False

        report_list = {
            ind : silent_call(self._check_reporting_for_individual_indicator, ind_df, skeleton_w_gdp, verbose = verbose)
            for ind, ind_df in df.groupby(self.series_key)
        }
        
        report_list = {
            ind: report_list.get(ind)
            for ind in series_list
        }

        if df[self.series_key].nunique() == 1:

            return next(iter(report_list.values()))
        
        else:
            
            return report_list