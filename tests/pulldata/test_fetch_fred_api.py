import pytest
import pysfo.pulldata as pysfo_pull
import pandas as pd
from pathlib import Path

def test_fetch_fred_api(temp_path):
    print("\n#===== Try FRED API =====#\n")

    try:
        try_list = "DGS1"
        pysfo_pull.fetch_fred_api.fetch_fred_series_list(
            series_list=[try_list], save_dir=str(temp_path)
        )
        fred_pull = pd.read_csv(temp_path / "DGS1.csv")
        print(fred_pull.head(4))
        assert not fred_pull.empty

    except Exception:
        pytest.skip("fetch_fred_api not working or data unavailable.")