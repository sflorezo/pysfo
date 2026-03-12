#%%

from ..configs import *
from .config import *
from .bis_derivatives import bisDerivatives
from .bis_dss import bisDSS
from .bis_ids import bisIDS
from . import dbnomicstools
from .fx_forwards import fxForwards
from . import cmns 
from .efa_row import EFARow 
from .frb_exchangerates import FRBExchangeRates
from . import fetch_fred_api
from .fof import FoF
from .fomc_dates import FOMCdates
from .fred import FREDcleaned
from .imf_ifs import imfIFS
from .imf_bop import imfBOP
# from .imf_weo import imfWEO
from . import interest_rates
from .lseg_api import lsgeData
from .wb_wdi import wbWDI
from .exchangerates import ExchangeRates
from . import other 
# from .geo_globals import geo_globals
from . import exceptions

#---- set_data path

from pysfo.configs import CONFIGS

DATA_RAW_PATH = (CONFIGS.get("PATHS") or {}).get("DATA_RAW_PATH")

if DATA_RAW_PATH and DATA_RAW_PATH.exists():
    set_data_path(DATA_RAW_PATH)
else:
    raise ValueError("DATA_RAW_PATH not found. Please set DATA_RAW_PATH in pysfo/configs/project_params.toml")

#---- available packages

__all__ = [
    'bisDerivatives',
    'bisDSS',
    'bisIDS',
    'dbnomicstools',
    'fxForwards',
    'cmns',
    'EFARow',
    'FRBExchangeRates',
    'fetch_fred_api',
    'FoF',
    'FOMCdates',
    'FREDcleaned',
    'imfIFS',
    'imfBOP',
    # 'imfWEO',
    'interest_rates',
    'lsgeData',
    'wbWDI',
    'ExchangeRates',
    'other',
    # 'geo_globals',
    'exceptions'
]