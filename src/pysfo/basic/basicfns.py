#%%

def flatten_list(lst):
    """
    Fastly flatten a list
    """
    
    out = []
    for x in lst:
        if isinstance(x, list):
            out.extend(flatten_list(x))
        else:
            out.append(x)
    return out

def statatab(data, na_report=False):
    """
    Equivalent to Stata's tabulate command with statistics.
    Adds a detailed missing-value breakdown if na_report=True.

    Parameters
    ----------
    data : list, np.ndarray, or pd.Series
        1D data vector.
    na_report : bool, default=False
        If True, include separate rows for None, np.nan, empty string (""), and other missings.

    Returns
    -------
    pd.DataFrame
        Table with value counts, percentages, and cumulative percentages.
    """

    import pandas as pd
    import numpy as np

    # Convert to Series for uniform handling
    if isinstance(data, (list, np.ndarray)):
        series = pd.Series(data)
    elif isinstance(data, pd.Series):
        series = data
    else:
        raise ValueError("Input data must be a list, numpy array, or Pandas Series")

    total_count = len(series)

    # Main (non-missing) tabulation
    non_missing = series.dropna()
    non_missing = non_missing[non_missing != ""]  # exclude empty strings
    count_df = non_missing.value_counts(dropna=False).reset_index()
    count_df.columns = ["Value", "Count"]
    count_df["Percentage"] = (count_df["Count"] / total_count) * 100
    count_df["Cumulative Percentage"] = count_df["Percentage"].cumsum()

    if na_report:
        # Initialize missing breakdown
        none_mask = series.map(lambda x: x is None)
        nan_mask = series.map(lambda x: isinstance(x, float) and np.isnan(x))
        empty_mask = series == ""
        known_missing_mask = none_mask | nan_mask | empty_mask
        other_missing_mask = series.isna() & ~known_missing_mask

        missing_summary = []

        def add_missing_row(label, mask):
            count = mask.sum()
            if count > 0:
                missing_summary.append({
                    "Value": label,
                    "Count": count,
                    "Percentage": count / total_count * 100
                })

        add_missing_row("(missing, None)", none_mask)
        add_missing_row("(missing, np.nan)", nan_mask)
        add_missing_row('(missing, "")', empty_mask)
        add_missing_row("(missing, other missing)", other_missing_mask)

        if missing_summary:
            missing_df = pd.DataFrame(missing_summary)
            # Compute cumulative percentages properly
            missing_df["Cumulative Percentage"] = (
                count_df["Percentage"].sum() + missing_df["Percentage"].cumsum()
            )
            count_df = pd.concat([count_df, missing_df], ignore_index=True)

    return count_df

def dupli_report(array, subset=None, list=False):
    """
    Report duplicate rows in the DataFrame.

    Parameters:
    - df: Pandas DataFrame
    - subset: List of columns to consider for identifying duplicates

    Returns:
    - DataFrame of duplicate rows
    """

    import pandas as pd

    if isinstance(array, pd.DataFrame) :
        
        df = array

        if subset is None :
            subset = df.columns.tolist()

        duplicates_bool = df.duplicated(subset=subset, keep=False)

    elif isinstance(array, pd.Series) :
        
        duplicates_bool = array.duplicated(keep=False)
    
    if list is False:
        count_df = duplicates_bool.value_counts().reset_index()
        count_df.columns = ['Duplicated', 'Count']
        count_df['Percentage'] = (count_df['Count'] / count_df['Count'].sum()) * 100
        count_df['Cumulative Percentage'] = count_df['Percentage'].cumsum()
        ret = count_df
    else:
        ret = df[duplicates_bool]
    
    return ret

def na_report(data, var=None, weight_col=None):
    """
    Missing-value report (unweighted or weighted).

    Behavior:
    - Unweighted (weight_col=None): If data is Series → report that series.
      If data is DataFrame → report all columns.
    - Weighted (weight_col provided): User MUST also provide `var`.
      Report only for `var`, using `weight_col` as weights.

    Parameters
    ----------
    data : pd.DataFrame or pd.Series
    var : str or None
        Column name to compute NA report for (required if weight_col is used).
    weight_col : str or None
        Name of numeric weight column.

    Returns
    -------
    pd.DataFrame
    """

    import pandas as pd
    import numpy as np

    # --- 1. handle list/ndarray ---
    if isinstance(data, (list, np.ndarray)):
        data = pd.Series(data)

    # --- 2. helper ---
    def report_series(series, w=None):
        if w is None:
            missing = series.isnull().sum()
            non_missing = series.notnull().sum()
            total = missing + non_missing
        else:
            missing = w[series.isnull()].sum()
            non_missing = w[series.notnull()].sum()
            total = missing + non_missing

        pct_missing = (missing / total) * 100 if total > 0 else np.nan
        pct_non_missing = (non_missing / total) * 100 if total > 0 else np.nan

        return pd.DataFrame({
            "Count": [missing, non_missing],
            "Percentage": [pct_missing, pct_non_missing]
        }, index=["Missing", "Non-Missing"])

    # =======================================================================
    # UNWEIGHTED MODE
    # =======================================================================
    if weight_col is None:

        # Series → single report
        if isinstance(data, pd.Series):
            return report_series(data)

        # DataFrame → all columns
        if isinstance(data, pd.DataFrame):
            out = []
            for col in data.columns:
                r = report_series(data[col])
                r.index = pd.MultiIndex.from_product([[col], r.index])
                out.append(r)
            return pd.concat(out)

        raise ValueError("Invalid data type.")

    # =======================================================================
    # WEIGHTED MODE (weight_col provided)
    # =======================================================================

    # Must be DataFrame
    if not isinstance(data, pd.DataFrame):
        raise ValueError("Weighted NA report requires DataFrame.")

    # var is required
    if var is None:
        raise ValueError("When weight_col is provided, `var` must also be provided.")

    # Check existence
    if var not in data.columns:
        raise ValueError(f"Variable `{var}` not found in DataFrame.")

    if weight_col not in data.columns:
        raise ValueError(f"weight_col `{weight_col}` not found in DataFrame.")

    # Must be numeric
    if not np.issubdtype(data[weight_col].dtype, np.number):
        raise ValueError("weight_col must be numeric.")

    # Weighted report only for `var`
    r = report_series(data[var], data[weight_col])
    r.index = pd.MultiIndex.from_product([[var], r.index])
    return r


def sumstats(df):
    """
    Summary statistics for the DataFrame.

    Parameters:
    - df: Pandas DataFrame

    Returns:
    - DataFrame with summary statistics
    """
    return df.describe(
        include='all',
        percentiles=[0.01, 0.10, 0.90, 0.99]
    )

def quarterly_list(start_quarter, end_quarter, step=1):

    import pandas as pd

    # Create a range of quarters
    quarter_range = pd.period_range(start=start_quarter, end=end_quarter, freq='Q')

    # Select quarters with the specified step
    quarter_list = quarter_range[::step]

    return quarter_list.astype(str).tolist()

def fullprint_df(df):
    """
    Print the entire contents of a DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to print.
    """

    import pandas as pd

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df)

def print_full_head(df, n=5):

    import pandas as pd

    # Save current settings
    prev_max_rows = pd.get_option("display.max_rows")
    prev_max_cols = pd.get_option("display.max_columns")

    # Remove limits
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)

    # Print first n rows
    print(df.head(n))

    # Restore previous settings
    pd.set_option("display.max_rows", prev_max_rows)
    pd.set_option("display.max_columns", prev_max_cols)

def colprint_df(df):

    import pandas as pd

    # Set the display option to show all columns
    with pd.option_context('display.max_columns', None, 'display.max_rows', 100):
        print(df)

def finish_ping(host = "google.com", ping_count = 4):

    import subprocess

    ping_command = ["ping", "-c", str(ping_count), host]
    subprocess.run(ping_command)

def weighted_mean(x, w):
    
    import numpy as np
    
    x = np.asarray(x)
    w = np.asarray(w)
    mask = ~np.isnan(x) & ~np.isnan(w)
    x = x[mask]
    w = w[mask]

    if w.sum() == 0 or len(x) == 0:
        return np.nan
    return np.sum(x * w) / np.sum(w)

def weighted_std(x, w):
    
    import numpy as np

    x = np.asarray(x)
    w = np.asarray(w)
    mask = ~np.isnan(x) & ~np.isnan(w)
    x = x[mask]
    w = w[mask]

    if w.sum() == 0:
        return np.nan
    mean = (x * w).sum() / w.sum()
    variance = ((w * (x - mean) ** 2).sum()) / w.sum()
    return np.sqrt(variance)

def weighted_quantile(values, weights, quantile):

    import numpy as np

    values = np.array(values)
    weights = np.array(weights)

    mask = ~np.isnan(values) & ~np.isnan(weights)
    values = values[mask]
    weights = weights[mask]

    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    cumulative_weight = np.cumsum(weights)
    total_weight = weights.sum()

    if total_weight == 0:
        return np.nan
    return values[np.searchsorted(cumulative_weight, quantile * total_weight)]

def print_all_df_columns(df):

    for col in df.columns:
        print(col)

def get_important_event_dates():

    covid_dates = {"start" : "2020-02-01", "end" : "2020-04-09"}
    debt_ceiling_june2023 = {'start': '2023-01-19', 'end': '2023-06-03'}

    all_dates = {
        "covid" : {
            "dates" : covid_dates,
            "description" : "Relevant covid date interval for debt market shock analysis.",
            "notes" : "Taken from Darmouni et al. (Don't remember exactly which one. Need to check this out.)"
        },
        "debt_ceiling_2023" : {
            "dates" : debt_ceiling_june2023,
            "description" : "Debt ceiling episode of Jan 2023 - Jun 2023.",
            "notes" : "Starts with U.S. Treasury hit the statutory debt limit in January 19th, and ends with Congress passing the Debt Responsability Act of 2023 in June 3rd."
        }
    }
    
    return all_dates


# %%
