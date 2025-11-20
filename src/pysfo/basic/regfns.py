#%%

def stars_from_pval(pval: float) -> str:
    """Return significance stars from a p-value."""
    if pval < 0.01:
        return "***"
    elif pval < 0.05:
        return "**"
    elif pval < 0.10:
        return "*"
    else:
        return ""

def create_table_for_model_results(coef_models_dict):

    """
    Creates a formatted table for displaying regression model results, including coefficients, 
    standard errors, and additional information if provided.

    Parameters:
    - coef_models_dict (dict): A dictionary where keys are model specifications and values are 
      model objects with attributes `params` and `bse` representing the coefficients and  
      standard errors, respectively.
    - main_add_rows_dict (dict, optional): A dictionary with the same keys as `coef_models_dict` 
      where each value is a dictionary specifying additional row details. 
      Expected keys in each inner dictionary:
        - "type": Type of additional data to include (currently only supports "add_model_se").
        - "from": The model object from which to pull additional standard error information.
    - footnotes_additional_rows (dict, optional): A dictionary with the same structure as 
      `main_add_rows_dict`, used to add additional rows like `rsquared_adj`. 
      Currently only supports adding adjusted R-squared information.

    Returns:
    - pandas.DataFrame: A concatenated DataFrame styled with multi-index columns displaying 
      coefficient names, their values, standard errors, number of observations, and R-squared 
      metrics if applicable. The table's bottom portion may include additional rows as 
      specified in `footnotes_additional_rows`.

    Raises:
    - ValueError: If `coef_models_dict` and `main_add_rows_dict` (if provided) do not have 
      the same length.
    - ValueError: If an unsupported type is specified in the `main_add_rows_dict` or 
      `footnotes_additional_rows`.

    Example usage:
    ```
    coef_models_dict = {
        "Model 1": model1,
        "Model 2": model2
    }
    main_add_rows_dict = {
        "Model 1": {"type": "add_model_se", "from": model1_se},
        "Model 2": {"type": "add_model_se", "from": model2_se}
    }
    table = create_table_for_model_results(coef_models_dict, main_add_rows_dict)
    print(table)
    ```
    """

    import pandas as pd
    from collections import OrderedDict
    import numpy as np

    #---- new

    toprows_dict = {}
    bottomrows_dict = {}

    for _, model_handler in coef_models_dict.items():
        
        name                = model_handler.get("name", None)
        model               = model_handler.get("model", None)
        options             = model_handler.get("options", None)
        footnote_options    = model_handler.get("footnote_options", None)

        # top of the table

        top_stats = {}

        for var in model.params.index:

            stats           = []
            stats_label     = []

            coef            = f"{model.params[var]:.3f}"
            stars           = stars_from_pval(model.pvalues[var])
            stats           += [coef]
            stats_label     += ["coef"]

            drop_se     = options.get("drop_se", None) if options else None

            if (drop_se is None) | (drop_se is False):
                
                se              = f"({model.bse[var]:.3f})"
                stats           += [se] if se else []
                stats_label     += ["se"]

            add_se      = options.get("add_model_se", None) if options else None

            if add_se:

                from_model  = add_se["from"]
                brackets    = add_se["brackets"] if add_se.get("brackets", None) else "()"
                l           = list(brackets)[0]
                r           = list(brackets)[1]

                se              = l + f"{from_model.bse[var]:.3f}" + r
                stars           = stars_from_pval(from_model.pvalues[var])
                stats           += [se] if se else []
                stats_label     += ["added_se"]
            
            stats[0] = stats[0] + stars

            options_coded   = ["drop_se", "add_model_se"]
            not_coded       = [opt for opt in options.keys() if opt not in options_coded] if options else None
            
            if len(not_coded) > 0:
                raise ValueError("Current version only considers options 'drop_se' and 'add_model_se'")

            top_stats[var] = stats

            toprows_dict[name] = pd.DataFrame(OrderedDict(top_stats)).T

        # bottom of the table

        bottom_stats = {}

        if footnote_options:
            
            report_stats = footnote_options.get("report_stats", None)
            report_stats_format = footnote_options.get("report_stats_format", None)
            additional_rows = footnote_options.get("additional_rows", None)
            
            for stat, format in zip(report_stats, report_stats_format) or []:
                
                stat_                = getattr(model, stat)
                bottom_stats[stat]   = format.strip().format(stat_)

            for i, addrows in enumerate(additional_rows) or []:
                bottom_stats[f"addrow_{i}"] = addrows

        else :

            bottom_stats["nobs"]      = f"{model.nobs:.0f}"
            bottom_stats["rsquared"]  = f"{model.rsquared:.3f}"

        bottomrows_dict[name] = pd.DataFrame([bottom_stats]).T

    top_table = pd.concat(toprows_dict, axis = 1)
    bottom_table = pd.concat(bottomrows_dict, axis = 1)

    # fix formats of top table

    top_table.columns = top_table.columns.set_names(["Spec", "Stat"])
    long_df = top_table.stack(level = "Stat")
    mapper = {i : lab for i, lab in enumerate(stats_label)}

    long_df.index = long_df.index.set_levels(
        long_df.index.levels[1].map(mapper), level=1
    )

    top_table = long_df
    
    # fix formats of bottom table

    bottom_table.columns = [item[0] for item in bottom_table.columns]

    # append top and bottom

    table = pd.concat([top_table, bottom_table], axis = 0)

    for col in table.columns:
        table[col] = np.where(table[col].isna(), "", table[col])

    return table

def export_table_to_latex(table,
                          colnames,
                          rownames,
                          drop_variables = "Intercept",
                          footnote_from_bottom = 1,
                          file = None):
    
    """
    Export a pandas DataFrame to a LaTeX formatted table, with customization options 
    such as dropping specified variables and adding horizontal lines.

    Parameters:
    - table (pandas.DataFrame): The DataFrame containing the data to be exported to LaTeX.
    - colnames (list of str): A list of column headers to be used in the LaTeX table.
    - rownames (list of str): A list of row names to replace the DataFrame's index.
    - drop_variables (str, optional): A regex pattern to match variables (rows) to drop 
      from the table before exporting. Default is "Intercept".
    - footnote_from_bottom (int, optional): The number of lines from the bottom where a 
      horizontal line (`\hline`) should be inserted for aesthetics. Default is 1.
    - file (str, optional): The file path where the LaTeX output should be saved. If not 
      specified, the LaTeX output will be printed to the console.

    Returns:
    - None

    This function processes a DataFrame to create a LaTeX formatted table. It modifies 
    the table by dropping specified variables, adjusting row names, and inserting 
    horizontal lines for better readability. It replaces the default LaTeX rules with 
    `\hline` for a consistent look throughout the table.

    If a file path is provided, the LaTeX code is written to the specified file; 
    otherwise, it is printed to the console.

    Example usage:
    ```
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    colnames = ["Column 1", "Column 2"]
    rownames = ["Row 1", "Row 2"]
    export_table_to_latex(df, colnames, rownames, file='output.tex')
    ```
    """

    import re

    table = table.copy()
    table.reset_index(inplace=True)
    table["index"] = table["index"].astype(str)

    if isinstance(drop_variables, (list, tuple)):
        
        drop_pattern = "|".join(f"(?:{re.escape(p)})" if not re.compile(p) else p
                                for p in drop_variables)
    else:
        drop_pattern = drop_variables

    drop = table["index"].str.contains(drop_pattern, regex=True)
    table = table.loc[~drop, :]

    table.loc[:,"index"] = rownames
    table.columns = [""] + colnames

    latex = table.to_latex(index=False, escape=False)
    ncols = len(table.columns)
    latex = latex.replace("{l" + "l"*(ncols-1) + "}", "{l" + "c"*(ncols-1) + "}")

    # --- post-process to insert \hline ---
    lines = latex.splitlines()

    # 2) add \hline above the footnote block
    # data rows start right after \midrule; so insertion point is:
    insert_idx = len(lines) - int(footnote_from_bottom)
    if 0 <= insert_idx < len(lines):
        lines.insert(insert_idx, r"\hline")
    
    out = "\n".join(lines)
    out = out.replace("\toprule", "\hline")
    out = out.replace("\midrule", "\hline")
    out = out.replace("\bottomrule", "\hline")

    if file:
        with open(file, "w") as f:
            f.write(out)
    else :
        print(out)
# %%
