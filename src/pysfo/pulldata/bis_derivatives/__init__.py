
import textwrap
from .bis_OTCderivatives_download import dbDownload
from . import master_upload

class bisDerivatives:

    """Interface to BIS Derivatives Statistics.
    This dataset includes
    1. OTC derivatives outstanding statistics
    2. Exchange-traded derivatives statistics
    
    The data for the Triennial Survey (which includes OTC turnover) is dismissed as it is too low frequency.
    """

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\

        To use this dataset, you first need to download the data from dbnomics and
        save it to your local machine, in the following directory:

        <RAW>/bis_derivatives_OTC_outstanding (for OTC deriatives statistics)
        <RAW>/bis_derivatives_ExchangeTraded (for Exchange Traded deriatives statistics)
                                            
        To do this, follow these steps:

        Step 1:
        --------
        Retrieve the data from dbnomics and store it. Example code:

            <ROOT_PACKAGE>.bisDerivatives.dbDownload().example_code()

        Step 2:
        --------
        After downloading, use the data via:

            <ROOT_PACKAGE>.bisDerivatives.get(...)

        -> To see available dbnomics filters:

            <ROOT_PACKAGE>.bisDerivatives.get_dbnomics_filters()
            
    """)

    @staticmethod
    def about():

        return (
            "Derivatives Statistics of the Bank for International Settlements (BIS)."
        )

    @staticmethod
    def print_instructions():

        return bisDerivatives._INSTRUCTION_TEMPLATE

    @staticmethod
    def get_OTC(DER_TYPE):
        return master_upload.get_OTC(DER_TYPE)
    
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
    
    class dbDownload(dbDownload):
        pass
    
    __all__ = [
        "about",
        "print_instructions",
        "get_OTC",
        "get_dbnomics_filters",
        "dbDownload",
    ]

__all__ = [
    "bisDerivatives"
]