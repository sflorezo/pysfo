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

    Parameters:
    - data: 1-dimensional array (list, vector, or Pandas Series)
    - na_report: if True, include a row for missing values
    
    Returns:
    - DataFrame with counts, percentage of observations, and cumulative percentage
    """

    import pandas as pd
    import numpy as np

    # Convert input data to a Pandas Series for consistency
    if isinstance(data, (list, np.ndarray)):
        series = pd.Series(data)
    elif isinstance(data, pd.Series):
        series = data
    else:
        raise ValueError("Input data must be a list, numpy array, or Pandas Series")

    total_count = len(series)

    # Drop NA for main tabulation
    non_missing = series.dropna()
    count_df = non_missing.value_counts().reset_index()
    count_df.columns = ['Value', 'Count']
    count_df['Percentage'] = (count_df['Count'] / total_count) * 100
    count_df['Cumulative Percentage'] = count_df['Percentage'].cumsum()

    if na_report:
        missing_count = series.isna().sum()
        if missing_count > 0:
            missing_row = pd.DataFrame({
                'Value': ['(Missing)'],
                'Count': [missing_count],
                'Percentage': [missing_count / total_count * 100],
                'Cumulative Percentage': [count_df['Percentage'].sum() + (missing_count / total_count * 100)]
            })
            count_df = pd.concat([count_df, missing_row], ignore_index=True)

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

def na_report(data):
    """
    Report missing values for each column in a DataFrame or for a 1-dimensional object.

    Parameters:
    - data: Pandas Series, DataFrame, list, or numpy array

    Returns:
    - DataFrame with multi-index (column, [Missing, Non-Missing]) and columns [Count, Percentage]
    """

    import pandas as pd
    import numpy as np

    def report_series(series):
        missing_count = series.isnull().sum()
        non_missing_count = series.notnull().sum()
        total_count = len(series)
        return pd.DataFrame({
            'Count': [missing_count, non_missing_count],
            'Percentage': [
                (missing_count / total_count) * 100,
                (non_missing_count / total_count) * 100
            ]
        }, index=['Missing', 'Non-Missing'])

    if isinstance(data, (list, np.ndarray)):
        data = pd.Series(data)

    if isinstance(data, pd.Series):
        return report_series(data)

    elif isinstance(data, pd.DataFrame):
        reports = []
        for col in data.columns:
            col_report = report_series(data[col])
            col_report.index = pd.MultiIndex.from_product([[col], col_report.index])
            reports.append(col_report)
        return pd.concat(reports)

    else:
        raise ValueError("Input data must be a list, numpy array, Pandas Series, or DataFrame")


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
