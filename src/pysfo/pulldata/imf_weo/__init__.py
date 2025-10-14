import textwrap
from .imf_weo_db_download import dbDownload
from . import upload_after_fetch

raise ImportError("pulldata.imf_weo is not yet implemented.")

class imfWEO:

    """Interface to IMF World Economic Outlook (WEO)."""

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\
        To implement.
    """)

    @staticmethod
    def about():

        return (
            "IMF World Economic Outlook (WEO)."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        file_path = get_data_path() / "imf_ifs"

        return imfWEO._INSTRUCTION_TEMPLATE.format(file_path = file_path)

    class dbDownload(dbDownload):
        pass

    @staticmethod
    def get(subdata, INDICATOR, FREQ):
        return upload_after_fetch.get(subdata, INDICATOR, FREQ)

__all__ = [
    "imfIFS"
]