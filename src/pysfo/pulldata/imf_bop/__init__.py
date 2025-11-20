
import textwrap
from .imf_bop_db_download import dbDownload
from . import master_upload

class imfBOP:

    """Interface to IMF Balance of Payment Statistics (BOP)."""

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\
        To use this dataset, you first need to download the data from dbnomics and
        save it to your local machine, in the following directory:

        {file_path}

        To do this, follow these steps:

        Step 1:
        --------
        Retrieve the data from dbnomics and store it. Example code:

            <ROOT_PACKAGE>.imfBOP.dbDownload().example_code()

        Step 2:
        --------
        After downloading, use the data via:

            <ROOT_PACKAGE>.imfBOP.get(subdata, INDICATOR, FREQ)

        -> 'subdata' is defined in the documentation of the main series included in
           the dataset. Example:

            documentation = <ROOT_PACKAGE>.imfBOP.dbDownload().main_series_documentation()
            print(documentation)

        -> To see available indicators and frequencies:

            documentation2 = <ROOT_PACKAGE>.imfBOP.dbDownload().subseries_documentation(subdata)
            print(documentation2)

        Note: You must complete Step 1 before requesting subdata documentation.
    """)

    @staticmethod
    def about():

        return (
            "Balance of Payment Statistics of the IMF."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        file_path = get_data_path() / "imf_bop"

        return imfBOP._INSTRUCTION_TEMPLATE.format(file_path = file_path)

    class dbDownload(dbDownload):
        pass

    @staticmethod
    def get(subdata, INDICATOR, FREQ, silent = False):
        return master_upload.get(subdata, INDICATOR, FREQ, silent)
    
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
        # dataset = "BOP"
        # subdata=subdata
        # INDICATOR=batch["VALUE"].unique()
        # FREQ="A"
        # summarized=True
        # report_percen=1
        # start_date="2010-01-01"
        # end_date = None
        # REF_AREA_all = False
        
        # import pysfo.pulldata as pysfo_pull
        # pysfo_pull.set_data_path("D:/Dropbox/80_data/raw") 

        #---- new reporter

        report_tables = dbTools.check_reporting(
            "IMF",
            "BOP",
            subdata, 
            INDICATOR, 
            FREQ, 
            summarized, 
            report_percen, 
            start_date, 
            end_date, 
            REF_AREA_all
        )

        #---- old reporter
        # check_reporter = checkReporting(subdata, INDICATOR, FREQ, summarized, report_percen, start_date, end_date, REF_AREA_all)

        return report_tables

__all__ = [
    "imfBOP"
]