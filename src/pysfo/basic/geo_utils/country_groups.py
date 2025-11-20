import pandas as pd
import json
import os
from pathlib import Path
import country_converter as coco
from typing import Any
import re
from pysfo.basic import convert_date_format

def assign_country_category(
    countries: pd.Series,
    src: str,
    category: str,
    date_var: Any = None
) -> pd.Series:
    """
    Assign membership to a given country category (e.g., EMU, EU, OECD).

    Parameters
    ----------
    countries : pd.Series
        Country identifiers (name, iso2, or iso3).
    src : str
        Identifier type. One of {'name', 'iso2', 'iso3'}.
    category : str
        Category to evaluate (e.g., "EMU", "EU", "ASIA", "TAX_HAVENS").
    date_var : optional
        Datetime-like variable (datetime, pandas Timestamp, or Period series).
        If provided, membership is evaluated by adoption or accession date.

    Returns
    -------
    pd.Series
        Series of 1 (member) or 0 (non-member), aligned with `countries`.
    """

    # #####
    # countries = pd.Series(["CZE", "DEU", "FRA", "CHN", "ESP"])
    # src="iso3"
    # category="EU"
    # date_var=convert_date_format(
    #     dates = ["2022Q1", "2019q4", "2000q1", "2021q3", "1999q4"],
    #     src = "string",
    #     to = "quarterly",
    #     string_example = "2020q2"
    # )
    # ####

    valid_src = {"name", "iso2", "iso3"}
    if src not in valid_src:
        raise ValueError(f"'src' must be one of {valid_src}")

    # Load category data (you can generalize the filename pattern later)
    path_to_json = Path(os.path.dirname(__file__)) / "json_files" / f"{category}_members.json"
    with open(path_to_json, "r") as f:
        category_data = json.load(f)

    if "members" not in category_data or "metadata" not in category_data:
        raise ValueError("category_data must contain 'metadata' and 'members' keys")

    members_df = pd.DataFrame(category_data["members"])
    as_of_date = category_data["metadata"].get("as_of_date", "unknown date")

    # --- Message ---
    if date_var is not None:
        print(f"Assigning {category} membership by date.")
    else:
        print(f"Assigning {category} membership as of {as_of_date}.")

    # --- Normalize identifiers ---
    countries_iso3 = None
    if src == "name":
        countries_iso3 = coco.convert(countries, src="name", to="ISO3", not_found=None)
        members_iso3 = members_df["iso3"].str.upper()
        member_set = set(members_iso3.dropna())
        result = pd.Series(countries_iso3).str.upper().isin(member_set).astype(int)
    else:
        member_set = set(members_df[src].dropna().str.upper())
        result = countries.astype(str).str.upper().isin(member_set).astype(int)

    # --- Optional: date-based membership ---
    if date_var is not None:
        # Convert to Series and handle period types
        if isinstance(date_var.dtype, pd.PeriodDtype):
            date_series = pd.Series(date_var).dt.to_timestamp()
        else:
            date_series = pd.Series(pd.to_datetime(date_var, errors="coerce"))

        # Automatically detect an accession/adoption date column
        date_cols = [
            c for c in members_df.columns
            if re.search(r"(adoption|accession|entry|join|membership).*year", c, flags=re.IGNORECASE)
        ]

        if date_cols:
            # use the first detected date-related column
            year_col = date_cols[0]
            year_map = dict(zip(members_df["iso3"].str.upper(), members_df[year_col]))

            # harmonize key identifiers
            if src == "name":
                key_series = pd.Series(countries_iso3).str.upper()
            else:
                key_series = countries.astype(str).str.upper()

            adoption_years = key_series.map(year_map)

            # ensure Series for .dt access
            date_series = pd.Series(date_series)

            result = ((~adoption_years.isna()) & (date_series.dt.year >= adoption_years)).astype(int)

    return result