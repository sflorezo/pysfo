
from pysfo.exceptions import DeprecatedModuleError
raise DeprecatedModuleError("[2026-03-11]: Module intended to be replaced by Tomas Laarits panel data.") 


#%% ========== Start Module ========== %%#

###
# import os
# from pysfo.pulldata import set_data_path
# set_data_path(os.getenv("DATA_RAW"))
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_on_rates")

class fxForwards:

    def __init__(self):

        from pysfo.pulldata import get_data_path
        
        fx_forwards_raw_path = get_data_path() / "fx_forwards"

        self.fx_forwards_raw_path = fx_forwards_raw_path

    def get_fx_forwards_prices(
            self,
    ):
        
        from .process import process_fx_forwards

        return process_fx_forwards(
            self.fx_forwards_raw_path,
        )
    
__all__ = [
    "fxForwards",
]