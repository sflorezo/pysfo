#%%

from .DM_G10 import dm_g10_rfr
from .EM_CORE import em_core_rfr
from .EM_NDF import em_ndf_rfr
from .EM_FRONTIER import em_frontier_rfr


rfr_list = {
    "DM_G10": dm_g10_rfr,
    "EM_CORE": em_core_rfr,
    "EM_NDF": em_ndf_rfr,
    "EM_FRONTIER": em_frontier_rfr
}
