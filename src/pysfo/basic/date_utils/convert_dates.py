import pandas as pd
import re
from typing import Any, Union

def convert_date_format(
    dates: Any,
    src: str,
    to: str,
    string_example: str | None = None
) -> pd.Series:
    """
    Convert dates between formats (datetime, monthly, quarterly, string).
    """

    dates = pd.Series(dates)

    valid_formats = {"datetime", "monthly", "quarterly", "string"}
    if src not in valid_formats or to not in valid_formats:
        raise ValueError(f"'src' and 'to' must be one of {valid_formats}")
    if src == "string" and not string_example:
        raise ValueError("When src='string', please provide string_example.")

    # Safely handle potential None
    example = (string_example or "").strip()

    # --- Step 1: convert input to datetime ---
    if src == "datetime":
        dt = pd.to_datetime(dates, errors="coerce")

    elif src == "monthly":
        dt = pd.PeriodIndex(dates, freq="M").to_timestamp()

    elif src == "quarterly":
        dt = pd.PeriodIndex(dates, freq="Q").to_timestamp()

    elif src == "string":
        # Quarterly like "2019q4" or "2019Q4"
        if re.fullmatch(r"\d{4}[Qq][1-4]", example):
            q = dates.astype(str).str.extract(r"(\d{4})[Qq]([1-4])")

            def _parse_quarter(x: pd.Series) -> Union[pd.Timestamp, None]:
                if pd.isna(x[0]) or pd.isna(x[1]):
                    return None
                return pd.Period(year=int(x[0]), quarter=int(x[1]), freq="Q").to_timestamp()

            dt = q.apply(_parse_quarter, axis=1)

        # Monthly like "2020m02", "2020M02", or "2020-02"
        elif re.fullmatch(r"\d{4}[Mm]\d{2}", example) or re.fullmatch(r"\d{4}-\d{2}", example):
            dt = pd.to_datetime(
                dates.astype(str)
                .str.replace("M", "-", regex=False)
                .str.replace("m", "-", regex=False),
                format="%Y-%m",
                errors="coerce",
            )

        # Daily like "2020-05-01"
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", example):
            dt = pd.to_datetime(dates, errors="coerce")

        else:
            raise ValueError(f"Unrecognized string pattern from example '{string_example}'.")

    else:
        raise ValueError(f"Unhandled src: {src}")

    # --- Step 2: convert to desired format ---
    dt = pd.Series(dt)
    if to == "datetime":
        return dt
    if to == "monthly":
        return dt.dt.to_period("M")
    if to == "quarterly":
        return dt.dt.to_period("Q")
    if to == "string":
        return dt.dt.strftime("%Y-%m-%d")

    return dt