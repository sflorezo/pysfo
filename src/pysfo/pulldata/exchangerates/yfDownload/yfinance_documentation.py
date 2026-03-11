#%%

PriceHistory_class_documentation = """
(as of 2026-02-23)

Parameters
----------
period : str
    Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    Default: 1mo
    Can combine with start/end e.g. end = start + period

interval : str
    Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    Intraday data cannot extend last 60 days

start : str
    Download start date string (YYYY-MM-DD) or datetime, inclusive.
    Default: 99 years ago
    E.g. for start="2020-01-01", first data point = "2020-01-01"

end : str
    Download end date string (YYYY-MM-DD) or datetime, exclusive.
    Default: now
    E.g. for end="2023-01-01", last data point = "2022-12-31"

prepost : bool
    Include Pre and Post market data in results?
    Default: False

actions : bool
    Default: True

auto_adjust : bool
    Adjust all OHLC automatically?
    Default: True

back_adjust : bool
    Back-adjusted data to mimic true historical prices

repair : bool
    Fixes price errors in Yahoo data: 100x, missing, bad dividend adjust.
    Default: False
    Full details at: Price Repair.

keepna : bool
    Keep NaN rows returned by Yahoo?
    Default: False

rounding : bool
    Optional: Round values to 2 decimal places?
    Default: False = use precision suggested by Yahoo!

timeout : None or float
    Optional: timeout fetches after N seconds
    Default: 10 seconds

raise_errors : bool
    If True, then raise errors as Exceptions instead of logging.
"""
# %%
