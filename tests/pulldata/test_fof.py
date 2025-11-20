import pytest
import pysfo.pulldata as pysfo_pull



def test_fof():

    print("\n#===== Try FRED FOF =====#\n")
    try:
        get_series = ["LM713061103", "LM883164115", "FL313161105", "FL313161110"]

        about = pysfo_pull.FoF.about()
        instructions = pysfo_pull.FoF.print_instructions()
        fof = pysfo_pull.FoF.get(
            series=get_series,
            frequency="Q",
        )
        print("About:\n" + "-"*len("About:") + "\n" + about + "\n")
        print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")
        print("Data glimpse:\n" + "-"*len("Data glimpse:"))
        print(fof.head(4))
        assert not fof.empty

    except FileNotFoundError:
        pytest.skip("FOF data missing.")