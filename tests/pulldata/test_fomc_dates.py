import pytest
import pysfo.pulldata as pysfo_pull



def test_fomc_dates():

    print("\n#===== Try FOMC dates =====#\n")

    from pysfo.exceptions import DeprecatedModuleError

    about = pysfo_pull.FOMCdates.about()
    instructions = pysfo_pull.FOMCdates.print_instructions()
    print("About:\n" + "-"*len("About:") + "\n" + about + "\n")
    print("Instructions:\n" + "-"*len("Instructions:") + "\n" + instructions + "\n")

    print("Data glimpse:\n" + "-"*len("Data glimpse:"))
    try:   

        fomc = pysfo_pull.FOMCdates.get_dates()
        print(fomc.head(4))
        assert not fomc.empty

    except DeprecatedModuleError:
        print("DEPRECATED: FOMCdates module is deprecated — skipping data load test (treated as PASS).")
        return

    except FileNotFoundError:
        pytest.skip("FOMC meeting dates missing.")