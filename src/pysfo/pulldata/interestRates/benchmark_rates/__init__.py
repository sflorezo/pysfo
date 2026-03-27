#%%

from .process import H_process_rfr
from typing import Union

# import os
# from pysfo.pulldata import set_data_path
# set_data_path(os.getenv("DATA_RAW"))
# benchmark_rates_dir = Path("/storage/Dropbox/80_data/raw/benchmark_on_rates")

class BenchmarkRates:

    def __init__(self):

        from pysfo.pulldata import get_data_path
        
        benchmark_rates_dir = get_data_path() / "benchmark_rates"

        self.benchmark_rates_dir = benchmark_rates_dir

    def get_rfr_panel(
            self,
            cty_group: Union[str, list, None] = None,
            ccy_iso3: Union[str, list, None] = None
    ):

        return H_process_rfr(
            self.benchmark_rates_dir,
            cty_group,
            ccy_iso3
        )
    
    @staticmethod
    def available_rfr_list():

        from .params.rfr import rfr_list

        return rfr_list

__all__ = [
    "BenchmarkRates",
]