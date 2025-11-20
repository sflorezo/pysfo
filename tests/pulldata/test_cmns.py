import pysfo.pulldata as pysfo_pull
import pytest

def test_cmns():
    
    print("\n#===== Try CMNS =====#\n")
    try:
        about = pysfo_pull.cmns.RestatementMatrices.about()
        instructions = pysfo_pull.cmns.RestatementMatrices.print_instructions()
        cmns = pysfo_pull.cmns.RestatementMatrices.get()
        
        print("About:\n" + "-"*len("About:") + "\n" + about + "\n")
        print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")
        print("Data glimpse:\n" + "-"*len("Data glimpse:"))
        print(cmns.head(10))
        assert not cmns.empty

    except FileNotFoundError:
        pytest.skip("CMNS data missing.")
