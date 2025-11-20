from pathlib import Path
import tomli


def load_tomli(path: str | Path) -> dict:
    """Load a TOML configuration file using tomli."""
    path = Path(path)
    with path.open("rb") as f:         # tomli requires binary mode
        return tomli.load(f)