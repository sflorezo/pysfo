#%%

import pysfo.pulldata as pysfo_pull
from setpaths import *
import pandas as pd
import country_converter as coco
from matplotlib import pyplot as plt
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

cc = coco.CountryConverter()

#%%========== check data ==========%%

dir(pysfo_pull)

efa = pysfo_pull.efa_row.get_efa_row(efa_row_dir = f"{raw}/efa_row", level = "agg")