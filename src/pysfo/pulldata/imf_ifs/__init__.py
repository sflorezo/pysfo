
import textwrap
from .imf_ifs_db_download import dbDownload
from . import master_upload

class imfIFS:

    """Interface to IMF International Financial Statistics (IFS)."""

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\
        To use this dataset, you first need to download the data from dbnomics and
        save it to your local machine, in the following directory:

        {file_path}

        To do this, follow these steps:

        Step 1:
        --------
        Retrieve the data from dbnomics and store it. Example code:

            <ROOT_PACKAGE>.imfIFS.dbDownload().example_code()

        Step 2:
        --------
        After downloading, use the data via:

            <ROOT_PACKAGE>.imfIFS.get(subdata, INDICATOR, FREQ)

        -> 'subdata' is defined in the documentation of the main series included in
           the dataset. Example:

            documentation = <ROOT_PACKAGE>.imfIFS.dbDownload().main_series_documentation()
            print(documentation)

        -> To see available indicators and frequencies:

            documentation2 = <ROOT_PACKAGE>.imfIFS.dbDownload().subseries_documentation(subdata)
            print(documentation2)

        Note: You must complete Step 1 before requesting subdata documentation.
    """)

    @staticmethod
    def about():

        return (
            "International Financial Statistics of the IMF."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        file_path = get_data_path() / "imf_ifs"

        return imfIFS._INSTRUCTION_TEMPLATE.format(file_path = file_path)

    class dbDownload(dbDownload):
        pass

    @staticmethod
    def get(subdata, INDICATOR, FREQ, silent = False):
        return master_upload.get(subdata, INDICATOR, FREQ, silent)
    
    @staticmethod
    def get_dbnomics_filters(filter = None):

        import os
        import re
        from pysfo.pulldata.dbnomicstools import get_filters

        current_dir = os.path.dirname(__file__)
        file = [file for file in os.listdir(current_dir) if re.findall(".customization", file)][0]
        json_metadata_path = os.path.join(current_dir, file)

        df_filters = get_filters(json_metadata_path, filter)

        return df_filters

    @staticmethod
    def check_reporting(
        subdata, 
        INDICATOR, 
        FREQ, 
        summarized = False, 
        report_percen = 1, 
        start_date = None, 
        end_date = None, 
        REF_AREA_all = False
    ):

        from pysfo.pulldata.dbnomicstools import dbTools

        # provider = "IMF"
        # dataset = "IFS"
        # subdata = subdata
        # series = INDICATOR
        # freq = FREQ
        # summarized = False 
        # report_percen = 1 
        # start_date = None 
        # end_date = None
        # REF_AREA_all = False

        # import pysfo.pulldata as pysfo_pull
        # pysfo_pull.set_data_path("D:/Dropbox/80_data/raw") 

        report_tables = dbTools.check_reporting(
            provider = "IMF",
            dataset = "IFS",
            subdata = subdata, 
            series = INDICATOR, 
            freq = FREQ, 
            summarized = summarized, 
            report_percen= report_percen, 
            start_date= start_date, 
            end_date = end_date, 
            REF_AREA_all = REF_AREA_all
        )

        return report_tables

__all__ = [
    "imfIFS"
]