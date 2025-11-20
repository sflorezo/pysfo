"""
Global configuration for pysfo.pulldata.
Manages the data path required to locate user datasets.
"""

from pathlib import Path

# ---- Module-level variable (global state) ----
_data_path: Path | None = None

def set_data_path(path: str | Path):
    """
    Set the global data directory for the package.

    Parameters
    ----------
    path : str | Path
        Directory path where raw or processed data are stored.
    """
    global _data_path

    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise RuntimeError(f"Data path does not exist: {p}")

    _data_path = p
    
def get_data_path() -> Path:
    """
    Return the global data directory set by the user.

    Raises
    ------
    RuntimeError
        If the data path has not been set yet.
    """
    if _data_path is None:
        raise RuntimeError(
            "Data path not set. Please call `set_data_path('/path/to/data')` first.\n\n"
            "Example:\n"
            "    import pysfo.pulldata as pysfo_pull\n"
            "    pysfo_pull.set_data_path('/Users/you/data/raw')"
        )

    return _data_path
