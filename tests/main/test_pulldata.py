import os
import pytest
import pandas as pd
import pysfo.pulldata as pysfo_pull
from setpaths import *
from dotenv import load_dotenv
import country_converter as coco

cc = coco.CountryConverter()

# ========== setup env ==========

@pytest.fixture(scope="session", autouse=True)
def setup_env():
    home_dotenv = os.path.expanduser("~/.env")
    load_dotenv(home_dotenv)

    pysfo_pull.set_api_keys(api_name="fred", api_key=os.getenv("FRED_API_KEY"))
    pysfo_pull.set_api_keys(api_name="openai", api_key=os.getenv("OPENAI_API_KEY"))


# ========== tests ==========


def test_see_all_pulls():

    print("\n#===== See all current data pulls =====#\n")
    print(pysfo_pull.__all__)
    assert pysfo_pull.__all__, "__all__ is empty!"

def test_cmns():
    print("\n#===== Try CMNS =====#\n")
    try:
        cmns = pysfo_pull.cmns.get_restated_matrices(cmns_path=f"{raw}/cmns")
        print(cmns.head(10))
        assert not cmns.empty
    except FileNotFoundError:
        pytest.skip("CMNS data missing.")


@pytest.mark.parametrize("level", ["agg", "regions", "disagg"])
def test_efa_row(level):
    print(f"\n#===== Try EFA ROW - {level} =====#\n")
    try:
        efa = pysfo_pull.efa_row.get_efa_row(
            efa_row_dir=f"{raw}/efa_row", level=level
        )
        print(efa.head(4))
        assert not efa.empty
    except FileNotFoundError:
        pytest.skip(f"EFA row level {level} missing.")


def test_exchangerates():
    print("\n#===== Try FRB H10 Exchange rates =====#\n")
    try:
        er = pysfo_pull.exchangerates.get_exchangerates(FRB_H10_dir=f"{raw}/FRB_H10")
        print(er.head(4))
        assert not er.empty
    except FileNotFoundError:
        pytest.skip("FRB H10 Exchange rates missing.")


def test_fetch_fred_api(tmp_path):
    print("\n#===== Try FRED API =====#\n")
    try:
        try_list = "DGS1"
        pysfo_pull.fetch_fred_api.fetch_fred_series_list(
            series_list=[try_list], save_dir=str(tmp_path)
        )
        fred_pull = pd.read_csv(tmp_path / "DGS1.csv")
        print(fred_pull.head(4))
        assert not fred_pull.empty
    except Exception:
        pytest.skip("fetch_fred_api not working or data unavailable.")


def test_fof():
    print("\n#===== Try FRED FOF =====#\n")
    try:
        get_series = ["LM713061103", "LM883164115", "FL313161105", "FL313161110"]
        fof = pysfo_pull.fof.get_fof_series(
            fred_flowoffunds_dir=f"{raw}/fred_flowoffunds",
            series=get_series,
            frequency="Q",
        )
        print(fof.head(4))
        assert not fof.empty
    except FileNotFoundError:
        pytest.skip("FOF data missing.")

def test_fomc_dates():
    print("\n#===== Try FOMC dates =====#\n")
    try:
        fomc = pysfo_pull.fomc_dates.get_fomc_meeting_dates(
            FOMC_meeting_dates_path=f"{raw}/FOMC_meeting_dates"
        )
        print(fomc.head(4))
        assert not fomc.empty
    except FileNotFoundError:
        pytest.skip("FOMC meeting dates missing.")


def test_fred_yields():
    print("\n#===== Try FRED downloaded data =====#\n")
    try:
        yields = pysfo_pull.fred.get_fred_yieldseries(fred_api_dir=f"{raw}/fred_api")
        print(yields.tail(4))
        assert not yields.empty
    except FileNotFoundError:
        pytest.skip("FRED yieldseries missing.")


def test_other_series():
    print("\n#===== Try OTHER series =====#\n")
    print("Not yet implemented.")
    pytest.skip("Other series not yet implemented.")