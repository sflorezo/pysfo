#%%

###
# import os
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_on_rates")

class fxTLaarits:

    def __init__(self):

        from pysfo.pulldata import get_data_path
        
        fx_forwards_raw_path = get_data_path() / "fx_prices_and_derivatives"

        self.fx_forwards_raw_path = fx_forwards_raw_path

    def get_fx_panel(
            self,
    ):
        
        from .process import H_get_fx_panel

        return H_get_fx_panel(
            file_path = self.fx_forwards_raw_path / "tlaarits_jan2026/G10_jan2026.xlsx",
        )
    
    def get_fx_tickers(
            self,
    ):
        
        from .process import H_get_fx_tickers

        return H_get_fx_tickers(
            file_path = self.fx_forwards_raw_path / "tlaarits_jan2026/G10_jan2026.xlsx",
        )
        
__all__ = [
    "fxTLaarits",
]