#%%

import textwrap
from . yfDownload import yfDownload
from . import master_upload
from typing import cast, Union

class ExchangeRates:

    """Interface to ER data."""

    @staticmethod
    def list_available_ccy_groups():

        from .params.ccy_pairs import ccy_pairs

        return ccy_pairs

    @staticmethod
    def get(
        interval : str,
        ccy_group : Union[str, None] = None, 
    ):
        
        return master_upload.H_get(
            interval = interval,
            ccy_group = ccy_group, 
        )
    
    @staticmethod
    def get_yfinance_documentation(filter = None):

        from .yfDownload.yfinance_documentation import PriceHistory_class_documentation

        yfinance_docs = {
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
        
__all__ = [
    "ExchangeRates"
]
# %%
