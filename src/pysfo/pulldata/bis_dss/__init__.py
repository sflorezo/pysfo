
import textwrap
from .bis_dss_db_download import dbDownload
from . import master_upload

class bisDSS:

    """Interface to BIS Debt Securities Statistics (DSS)."""

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\

        To use this dataset, you first need to download the data from dbnomics and
        save it to your local machine, in the following directory:

        <RAW>/{file_path}

        To do this, follow these steps:

        Step 1:
        --------
        Retrieve the data from dbnomics and store it. Example code:

            <ROOT_PACKAGE>.bisDSS.dbDownload().example_code()

        Step 2:
        --------
        After downloading, use the data via:

            <ROOT_PACKAGE>.bisDSS.get(REF_AREA, FREQ)

        -> To see available REF_AREA and FREQ:

            <ROOT_PACKAGE>.bisDSS.get_dbnomics_filters()
            

        Note: You must complete Step 1 before requesting subdata documentation.
    """)

    @staticmethod
    def about():

        return (
            "Debt Security Statistics of the Bank for International Settlements (BIS)."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        file_path = get_data_path() / "bis_dss"

        return bisDSS._INSTRUCTION_TEMPLATE.format(file_path = file_path)

    class dbDownload(dbDownload):
        pass

    @staticmethod
    def get(REF_AREA, FREQ):
        return master_upload.get(REF_AREA, FREQ)
    
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

__all__ = [
    "bisDSS"
]