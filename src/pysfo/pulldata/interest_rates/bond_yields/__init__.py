#%%

from typing import List, Union, Dict

class BondYields:

    @staticmethod
    def get_available_yields(maturity : Union[str, List[str]]) -> Dict:
        
        from .params.yields_10y import available_10Y_yields
        from .params.yields_2y import available_2Y_yields


        available_yields = {
            "10Y" : available_10Y_yields,
            "2Y" : available_2Y_yields,
        }

        maturity_list = [maturity] if isinstance(maturity, str) else maturity

        _available = list(available_yields.keys())
        _return_error = any([mat not in _available for mat in maturity_list])

        _available = "\n".join(["- " + mat for mat in _available])

        if _return_error:
            _msg = (
                "Selected unavailable maturities. Please choose from the following: \n" + _available
            )
            raise ValueError(_msg)

        available_yields = {
            "10Y" : available_10Y_yields,
            "2Y" : available_2Y_yields,
        }

        available_yields = {mat : vals for mat, vals in available_yields.items() if mat in maturity_list}
        
        return available_yields

__all__ = [
    "BondYields"
]