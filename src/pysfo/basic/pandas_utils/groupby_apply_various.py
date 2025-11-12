#%%
 


def groupby_apply_various(group, operations):

    """
    Apply multiple operations to a grouped pandas DataFrame.

    This function enables flexible application of different functions
    to one or more specified columns within each group of a pandas
    GroupBy object. Each operation is defined in a dictionary specifying
    the function, which columns to use as arguments, and any optional
    additional arguments.

    Parameters
    ----------
    group : pandas.DataFrame or pandas.Series
        A group from a pandas GroupBy operation.
    operations : dict
        Dictionary specifying the operations to apply. Keys are the names
        of the output fields. Each value must itself be a dictionary with:
        
        - 'fn' : callable
            The function to apply.
        - 'df_argnames' : str or list of str
            Column name(s) from the group to pass as positional arguments.
        - 'otherargs' : optional
            Additional argument(s) to pass to the function.

    Returns
    -------
    pandas.Series
        A Series where each element corresponds to the result of one
        specified operation.

    Raises
    ------
    TypeError
        If `operations` is not a dictionary, or if any operation
        specification is not a dictionary with the required keys.
    Exception
        If a function application fails, the corresponding output
        value is set to NaN and the exception is re-raised.

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> df = pd.DataFrame({
    ...     "group": ["A", "A", "B", "B"],
    ...     "x": [1, 2, 3, 4],
    ...     "y": [10, 20, 30, 40]
    ... })
    >>> operations = {
    ...     "mean_x": {"fn": np.mean, "df_argnames": "x"},
    ...     "sum_xy": {"fn": lambda x, y: np.sum(x + y),
    ...                "df_argnames": ["x", "y"]}
    ... }
    >>> df.groupby("group").apply(lambda g: groupby_apply_various(g, operations))
       mean_x  sum_xy
    group              
    A       1.5      33
    B       3.5      77

    Notes
    -----
    Last reviewed: 2025-09-25 (SFO)
    Status: ⚠️ Candidate for refactor
    """

    import pandas as pd
    import numpy as np
    import warnings
    
    warnings.warn(
        "[SFO-2025-09-25] groupby_apply_various may be outdated. There may be easier ways to groupby_apply to various columns. However, this function is very useful in datasets with multiple columns. AVOID ERASING THIS FUNCTION.",
        category=UserWarning,
        stacklevel=2
    )

    if not isinstance(operations, dict):
        raise TypeError("operations must be a dictionary")

    for key, val in operations.items():
        if not isinstance(val, dict):
            errormsg = f"Each value in operations must be a dict, with keys 'fn', 'df_argnames' and 'otherargs'\n"
            errormsg += f"- got {type(val)} for key '{key}'"
            raise TypeError(errormsg)

    stats = {}

    for out, oper in operations.items():
        fn = oper["fn"]
        df_argnames = oper["df_argnames"]

        if isinstance(df_argnames, str):
            df_argnames = [df_argnames]

        # Extract df_arguments from the group
        args = [group[arg].to_numpy() for arg in df_argnames]
        try:
            args = args + [oper["otherargs"]]
        except :
            pass

        # Call the function with unpacked args
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                stats[out] = fn(*args)
            except Exception as e:
                stats[out] = np.nan
                raise
    
    return pd.Series(stats)
    