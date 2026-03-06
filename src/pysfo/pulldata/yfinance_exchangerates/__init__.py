#%%

import textwrap
from . yfDownload import yfDownload
from . import master_upload
from typing import cast

class yfER:

    """Interface to yfinance ER data."""

    @staticmethod
    def list_available_ccy_groups():

        from .params.ccy_pairs import ccy_groups

        return ccy_groups

    @staticmethod
    def get(
        ccy_group, 
        downloaddate,
        period,
        interval
    ):
        
        return master_upload.H_get(
            ccy_group = ccy_group, 
            downloaddate = downloaddate,
            period = period,
            interval = interval
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
    "yfER"
]
# %%
