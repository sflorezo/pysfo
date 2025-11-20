
import textwrap
from .wb_wdi_db_download import dbDownload
from . import master_upload

class wbWDI:

    """Interface to World Bank World Development Indicators (WDI)."""

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\
               
        To use this dataset, you first need to download the data from dbnomics and
        save it to your local machine, in the following directory:

        {file_path}

        To do this, follow these steps:

        Step 1:
        --------
        Retrieve the data from dbnomics and store it. Example code:

            <ROOT_PACKAGE>.wbWDI.dbDownload().example_code()

        Step 2:
        --------
        After downloading, use the data via:

            <ROOT_PACKAGE>.wbWDI.get(indicator, frequency)

        -> 'indicator' is defined in the documentation of the main series included in
           the dataset. Example:

            documentation = <ROOT_PACKAGE>.wbWDI.dbDownload().main_series_documentation()
            print(documentation)

        -> To see available indicators and frequencies:

            All indicators in documentation. In Oct 2025, frequencies are only Annual.

        Note: You must complete Step 1 before requesting subdata documentation.
    """)
    
    @staticmethod
    def about():

        return (
            "World Development Indicators (WDI) of the World Bank."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        file_path = get_data_path() / "wb_wdi"

        return wbWDI._INSTRUCTION_TEMPLATE.format(file_path = file_path)

    class dbDownload(dbDownload):
        pass

    @staticmethod
    def get(indicator, frequency = None, silent = False):
        return master_upload.get(indicator, frequency, silent)

__all__ = [
    "wbWDI"
]