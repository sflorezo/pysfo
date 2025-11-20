#%%

import textwrap

class RestatementMatrices:

    """Interface to get Restatement Matrices (CMNS, 2021)."""

    @staticmethod
    def about():

        return (
            "Restatement Matrices published in 'Redrawing the Map of Capital Flows' (Coppola, Maggiori, Nieman, Schreger; 2021)."
        )

    @staticmethod
    def print_instructions():

        from .config import get_data_path

        file_path = get_data_path() / "cmns/Restatement_Matrices.dta"

        return textwrap.dedent(f"""\
            To use this dataset, download the Restatement Matrices from the GCAP webpage and store them so that the main data path is
                               
            "{file_path}"

        """) 
    
    @staticmethod
    def get():
        
        from .config import get_data_path
        import pandas as pd
        import os

        # Define the file path
        file_path = get_data_path() / "cmns/Restatement_Matrices.dta"
    
        # Check if the file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The required path '{file_path}' does not exist. Please check the instructions for downloading the data.'")

        # Read the Stata file if it exists
        df = pd.read_stata(file_path)
        
        return df