import pytest
import pysfo.pulldata as pysfo_pull

@pytest.mark.parametrize("level", ["agg", "regions", "disagg"])
def test_efa_row(level):

    print(f"\n#===== Try EFA ROW - {level} =====#\n")
    try:
        about = pysfo_pull.EFARow.about()
        instructions = pysfo_pull.EFARow.print_instructions()
        efa = pysfo_pull.EFARow.get(level = level)

        print("About:\n" + "-"*len("About:") + "\n" + about + "\n")
        print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")
        print("Data glimpse:\n" + "-"*len("Data glimpse:"))
        print(efa.head(4))
        assert not efa.empty

    except FileNotFoundError:
        pytest.skip(f"EFA row level {level} missing.")
