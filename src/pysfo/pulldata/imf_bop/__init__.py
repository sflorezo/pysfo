
import textwrap
from .imf_bop_db_download import dbDownload
from .check_reporting import check_reporting
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
    def check_reporting(subdata, INDICATOR, FREQ, report_percen = 1, start_date = None, end_date = None):
        return check_reporting(subdata, INDICATOR, FREQ, report_percen, start_date, end_date)

__all__ = [
    "imfIFS"
]