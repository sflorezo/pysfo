#%%

def get_restated_matrices(cmns_path):

    import pandas as pd
    import os

    # Define the file path
    file_path = f"{cmns_path}/Restatement_Matrices.dta"

    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The required file 'Restatement_Matrices.dta' is missing in '{cmns_path}'")

    # Read the Stata file if it exists
    df = pd.read_stata(file_path)
    
    return df