import pandas as pd
from pathlib import Path

def configure_pandas_display(max_cols=500, max_rows=300):
    """Set preferred pandas display options."""
    pd.set_option("display.max_columns", max_cols)
    pd.set_option("display.max_rows", max_rows)


def save_parquet(df: pd.DataFrame, path, *, index=False):
    """Save a DataFrame to Parquet with standard settings."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", compression="zstd", index=index)

def load_parquet(path: str | Path) -> pd.DataFrame:
    """Load a DataFrame from Parquet."""
    return pd.read_parquet(path, engine="pyarrow")

import pandas as pd
from typing import Literal

def relocate_columns(
    df: pd.DataFrame,
    cols_to_move: list[str],
    anchor_col: str,
    how: Literal["after", "before"] = "after"
) -> pd.DataFrame:
    """
    Relocate one or more columns in a DataFrame relative to an anchor column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    cols_to_move : list of str
        Columns to relocate (in their current relative order).
    anchor_col : str
        Column name to serve as insertion reference point.
    how : {"after", "before"}, default "after"
        Whether to insert the relocated columns after or before the anchor.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns reordered accordingly.
    """
    # Defensive copy
    df = df.copy()

    # Validate presence
    missing = [c for c in cols_to_move + [anchor_col] if c not in df.columns]
    if missing:
        raise KeyError(f"Columns not found in DataFrame: {missing}")

    # Determine anchor position
    pos = df.columns.get_loc(anchor_col)
    if not isinstance(pos, int):
        raise TypeError(
            f"Unexpected type from get_loc: {type(pos).__name__}. "
            "Anchor column labels may not be unique."
        )

    insert_at = pos + 1 if how == "after" else pos

    # Build new order
    all_cols = list(df.columns)
    remaining = [c for c in all_cols if c not in cols_to_move]
    new_order = (
        remaining[:insert_at]
        + cols_to_move
        + remaining[insert_at:]
    )

    return df[new_order]