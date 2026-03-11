from .yfinance_er_download import fetch_and_save_yfinance_er_by_ccy_group

class yfDownload:

    def __init__(self):

        from pysfo.pulldata import get_data_path
        
        self._base_dir = get_data_path() / "yfinance_exchangerates"

    def fetch_and_save_yfinance_er_by_ccy_group(self, ccy_group, csv_save_dir, force_fetch = False, **kwargs):
        
        root_raw_yfinance_er_path = self._base_dir

        fetch_and_save_yfinance_er_by_ccy_group(
            root_raw_yfinance_er_path,
            ccy_group,
            csv_save_dir, 
            force_fetch,
            **kwargs
        )
    
    def example_code(self):

        example_code = (

            """
            #========== packages and paths ==========#

            To Write.

            """
        )
        
        return example_code