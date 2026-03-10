from .EM_NDF_INR import inr_rfr
from .EM_NDF_KRW import krw_rfr
from .EM_NDF_IDR import idr_rfr
from .EM_NDF_PHP import php_rfr
from .EM_NDF_MYR import myr_rfr
from .EM_NDF_TWD import twd_rfr
from .EM_NDF_CNY import cny_rfr

em_ndf_rfr =  {
    "INR": inr_rfr,
    "KRW": krw_rfr,
    "IDR": idr_rfr,
    "PHP": php_rfr,
    "MYR": myr_rfr,
    "TWD": twd_rfr,
    "CNY": cny_rfr,
}