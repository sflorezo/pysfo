#%%

from pysfo.pulldata import interestRates
from pysfo.configs import CONFIGS
import pandas as pd

#%%

# Currency order matches example_final_data (DKK omitted — no RFR in source)
CCY_ORDER = ["aud", "cad", "chf", "eur", "gbp", "jpy", "nok", "nzd", "sek", "usd"]

# All benchmarks to extract per currency, each becomes its own column.
# Format: (benchmark_name, tenor) -> column suffix: benchmark_lower_tenor
BENCHMARKS = {
    "aud": [("AONIA",     "on")],
    "cad": [("CORRA",     "on")],
    "chf": [("SARON",     "on")],
    "eur": [("ESTR",      "on"), ("EONIA",     "on"), ("EURIBOR", "3m")],
    "gbp": [("SONIA",     "on"), ("GBP_LIBOR", "3m")],
    "jpy": [("TONAR",     "on"), ("JPY_LIBOR", "3m")],
    "nok": [("NOWA",      "on")],
    "nzd": [("NZONIA",    "on")],
    "sek": [("SWESTR",    "on")],
    "usd": [("SOFR",      "on"), ("USD_LIBOR", "on"), ("USD_LIBOR", "1m"), ("USD_LIBOR", "3m")],
}


def clean_rfr() -> pd.DataFrame:

    df = interestRates.BenchmarkRates().get_rfr_panel()

    # Normalise column names
    df.columns = df.columns.str.strip()

    # Parse date
    df["date"] = pd.to_datetime(df["date"])

    wide_frames = []

    for ccy in CCY_ORDER:
        ccy_upper = ccy.upper()
        ccy_df = df[df["ccy"] == ccy_upper].copy()

        if ccy_df.empty:
            print(f"  [{ccy_upper}] No data found — skipping.")
            continue

        for benchmark, tenor in BENCHMARKS[ccy]:
            # e.g. "usd_benchmark_sofr_on", "usd_benchmark_usd_libor_3m"
            col_name = f"{ccy}_benchmark_{benchmark.lower().replace('_', '_')}_{tenor}"

            subset = ccy_df[
                (ccy_df["benchmark"] == benchmark) & (ccy_df["tenor"] == tenor)
            ][["date", "rate"]].drop_duplicates("date").set_index("date")["rate"]

            subset.name = col_name
            wide_frames.append(subset)
            print(f"  [{ccy_upper}] {col_name}  — {len(subset):,} obs")

    # Combine all series into a wide DataFrame
    wide = pd.concat(wide_frames, axis=1, sort=True)
    wide = wide.sort_index()
    wide.index.name = "date"
    wide = wide.reset_index()

    print(f"\nOutput shape: {wide.shape}")
    print(f"Date range:  {wide['date'].min().date()} → {wide['date'].max().date()}")
    print(f"Columns: {list(wide.columns)}")

    return wide
 
 
# %%
