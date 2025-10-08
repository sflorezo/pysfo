
from .dbnomics_download import dbDownload
from . import upload_after_fetch

class imfIFS:

    @staticmethod
    def about():

        return (
            "International Financial Statistics of the IMF."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        file_path = get_data_path() / "cmns/Restatement_Matrices.dta"

        return (
            f"To use this dataset, download the Restatement Matrices from the GCAP webpage and store them so that the main data path is\n"
            f"{file_path}\n"
        )

    class dbDownload(dbDownload):
        pass

    @staticmethod
    def get(subdata, INDICATOR, FREQ):
        return upload_after_fetch.get(subdata, INDICATOR, FREQ)

__all__ = [
    "imfIFS"
]