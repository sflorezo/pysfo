from pysfo.pulldata.dbnomicstools.config import get_filters
from pysfo.pulldata.dbnomicstools.check_timeseries_reporting.create_report import checkReporting

class dbTools:

    @staticmethod
    def get_filters(json_metadata_path, filter = None):
        return get_filters(json_metadata_path, filter)

    @staticmethod
    def check_reporting(
        provider,
        dataset,
        subdata, 
        series, 
        freq, 
        summarized = False, 
        report_percen = 1, 
        start_date = None, 
        end_date = None, 
        REF_AREA_all = False
    ):

        # import pysfo.pulldata as pysfo_pull
        # pysfo_pull.set_data_path("D:/Dropbox/80_data/raw") 

        check_reporter = checkReporting(
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
        )

        return check_reporter.create_report_tables()

__all__ = [
    'dbTools'
]