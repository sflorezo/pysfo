
import textwrap
from .imf_ifs_db_download import dbDownload
from . import upload_after_fetch

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
    def get(subdata, INDICATOR, FREQ):
        return upload_after_fetch.get(subdata, INDICATOR, FREQ)

__all__ = [
    "imfIFS"
]