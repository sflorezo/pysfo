import pytest
import pysfo.pulldata as pysfo_pull

def test_exchangerates():
    print("\n#===== Try FRB H10 Exchange rates =====#\n")

    try:
        about = pysfo_pull.FRBExchangeRates.about()
        instructions = pysfo_pull.FRBExchangeRates.print_instructions()
        er = pysfo_pull.FRBExchangeRates.get()

        print("About:\n" + "-"*len("About:") + "\n" + about + "\n")
        print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")
        print("Data glimpse:\n" + "-"*len("Data glimpse:"))
        print(er.head(4))
        assert not er.empty
        
    except FileNotFoundError:
        pytest.skip("FRB H10 Exchange rates missing.")
