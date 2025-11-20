import pytest
import pysfo.pulldata as pysfo_pull
from pprint import pprint

def test_fred_yields():

    print("\n#===== Try FRED downloaded data =====#\n")

    about = pysfo_pull.FREDcleaned.about()
    instructions = pysfo_pull.FREDcleaned.print_instructions()
    available = pysfo_pull.FREDcleaned.available_datasets()
    print("About:\n" + "-"*len("About:") + "\n" + about + "\n")
    print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")
    print("Available Datasets:\n" + "-"*len("Available Datasets:") + "\n")
    pprint(available)
    print("\n")

    print("Data glimpse:\n" + "-"*len("Data glimpse:"))
    try:
        yields = pysfo_pull.FREDcleaned.get.yieldseries()
        print(yields.tail(4))
        assert not yields.empty
    except FileNotFoundError:
        pytest.skip("FRED yieldseries missing.")