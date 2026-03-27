from .EM_CORE_BRL import brl_rfr
from .EM_CORE_MXN import mxn_rfr
from .EM_CORE_ZAR import zar_rfr
from .EM_CORE_TRY import try_rfr
from .EM_CORE_PLN import pln_rfr
from .EM_CORE_HUF import huf_rfr
from .EM_CORE_CZK import czk_rfr
from .EM_CORE_CLP import clp_rfr
from .EM_CORE_COP import cop_rfr

em_core_rfr = {
    "BRL": brl_rfr,
    "MXN": mxn_rfr,
    "ZAR": zar_rfr,
    "TRY": try_rfr,
    "PLN": pln_rfr,
    "HUF": huf_rfr,
    "CZK": czk_rfr,
    "CLP": clp_rfr,
    "COP": cop_rfr,
}