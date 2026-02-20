#%%

import textwrap
from . yfDownload import yfDownload
from . import master_upload
from typing import cast

class yfER:

    """Interface to yfinance ER data."""

    _INSTRUCTION_TEMPLATE = textwrap.dedent("""\

        To Write.
                                            
    """)

    @staticmethod
    def about():

        return (
            "YFinance ER data."
        )

    @staticmethod
    def print_instructions():

        from ..config import get_data_path

        return yfER._INSTRUCTION_TEMPLATE
    
    @staticmethod
    def get_yfinance_documentation(filter = None):

        from .yfDownload.exchangerates_params import available_ccy_pairs
        from .yfDownload.yfinance_documentation import PriceHistory_class_documentation

        yfinance_docs = {
            "available_currency_pairs" : available_ccy_pairs,
            "PriceHistory_class_documentation" : PriceHistory_class_documentation,
        }

        if filter in yfinance_docs.keys():

            return yfinance_docs[cast(str, filter)]

        elif filter is None:

            return yfinance_docs
        
        else :
            
            _available_filters = list(yfinance_docs.keys())
            _available_filters = "\n".join(["- " + item for item in _available_filters])
            _msg = (
                "filter not found. Please select one of the following:\n"
                f"{_available_filters}"
            )

            raise ValueError(_msg)
        
    class yfDownload(yfDownload):
        pass

    @staticmethod
    def get(ccy_group, **kwargs):
        
        return master_upload.get(ccy_group = ccy_group, **kwargs)
        
    __all__ = [
        "about",
        "print_instructions",
        "get",
        "get_yfinance_documentation",
    ]


__all__ = [
    "yfER"
]
# %%
