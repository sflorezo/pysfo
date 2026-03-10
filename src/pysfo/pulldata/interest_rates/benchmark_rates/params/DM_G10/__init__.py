#%%

from .DM_G10_AUD import aud_rfr
from .DM_G10_CAD import cad_rfr
from .DM_G10_CHF import chf_rfr
from .DM_G10_EUR import eur_rfr
from .DM_G10_GBP import gpb_rfr
from .DM_G10_JPY import jpy_rfr
from .DM_G10_NOK import nok_rfr
from .DM_G10_NZD import nzd_rfr
from .DM_G10_SEK import sek_rfr
from .DM_G10_USD import usd_rfr

dm_g10_rfr = {
    "USD": usd_rfr,
    "EUR": eur_rfr,
    "GBP": gpb_rfr,
    "JPY": jpy_rfr,
    "CHF": chf_rfr,
    "CAD": cad_rfr,
    "AUD": aud_rfr,
    "NOK": nok_rfr,
    "SEK": sek_rfr,
    "NZD": nzd_rfr,
}