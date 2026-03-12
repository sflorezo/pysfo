#%%

###
# import os
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_on_rates")

class fxForwards:

    def __init__(self):

        from pysfo.pulldata import get_data_path
        
        fx_forwards_raw_path = get_data_path() / "fx_forwards"

        self.fx_forwards_raw_path = fx_forwards_raw_path

    def get_fx_forwards_prices(
            self,
    ):
        
        from .process import H_get_fx_forwards_prices

        return H_get_fx_forwards_prices(
            file_path = self.fx_forwards_raw_path / "tlaarits_jan2026/G10_jan2026.xlsx",
        )
    
    def get_fx_forwards_tickers(
            self,
    ):
        
        from .process import H_get_fx_forwards_tickers

        return H_get_fx_forwards_tickers(
            file_path = self.fx_forwards_raw_path / "tlaarits_jan2026/G10_jan2026.xlsx",
        )
    

    
__all__ = [
    "fxForwards",
]