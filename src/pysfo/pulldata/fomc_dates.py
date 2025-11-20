#%%========== module parameters ==========%%#

# deprecated/active
MODULE_STATUS = "deprecated"

#%%========== data retriever ==========%%#

class FOMCdates:

    @staticmethod
    def about():

        return (
            "Useful methods for retrieving FOMC dates, and merging them with other datasets."
        )

    @staticmethod
    def print_instructions():

        from .config import get_data_path

        file_path = get_data_path() / "FOMC_meeting_dates"

        return (
            f"To use this dataset, get the FOMC meeting dates data in the directory\n"
            f"{file_path}\n"
        )
    
    @staticmethod
    def check_status():
        
        from pysfo.exceptions import DeprecatedModuleError

        if MODULE_STATUS == "deprecated":

            raise DeprecatedModuleError("This method is currently deprecated. It is necessary to get a new source for the time series of FOMC meetings")
        
        elif MODULE_STATUS == "active":

            pass

    @staticmethod
    def get_dates():

        from .config import get_data_path
        import pandas as pd
        import os
        from pysfo.exceptions import DeprecatedModuleError
        
        try:

            # check status
            FOMCdates.check_status()

            # Construct the full path to the CSV file
            FOMC_meeting_dates_path = get_data_path() / "FOMC_meeting_dates"
            file_path = f"{FOMC_meeting_dates_path}/fomc_meeting_dates_2018_onward.csv"
            
            # Check if the file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"The required file '{file_path}' is missing.")

            meetings = pd.read_csv(f"{FOMC_meeting_dates_path}/fomc_meeting_dates_2018_onward.csv") 
            meetings.columns = meetings.columns.str.lower()
            meetings["date"] = pd.to_datetime(meetings["date"])
            return meetings
        
        except DeprecatedModuleError:
            raise  
        
        except Exception as e:
            raise RuntimeError("Error while loading FOMC meeting dates.") from e

    @staticmethod
    def merge_fomc_meeting_dates(df, datevar):
        
        import numpy as np
        import pandas as pd
        
        print("Merging FOMC meeting dates to closest meeting, both before and after 'datevar'")

        meetings = FOMCdates.get_dates()
        meetings["fomc_meeting"] = 1

        contentcols = df.drop(columns = datevar).columns.to_list()
        
        df = df.merge(meetings, 
                    how = "outer",
                    left_on = datevar, 
                    right_on = "date",
                    indicator = True)
        
        df["report_date"] = np.where(
            df["_merge"] == "right_only", 
            df["date"], 
            df[datevar]
        )

        df = df[[datevar, "fomc_meeting"] + contentcols]
        
        df = df.sort_values(datevar).reset_index(drop = True)
        df["fomc_meeting"] = df["fomc_meeting"].fillna(0).astype(int)
        
        df = df.sort_values('report_date').copy()
        meetings = meetings.sort_values('date')

        prev_merge = pd.merge_asof(
            df[['report_date']],
            meetings[["date"]].rename(columns={'date':'prev_meeting'}),
            left_on='report_date', right_on='prev_meeting',
            direction='backward'
        )

        next_merge = pd.merge_asof(
            df[['report_date']],
            meetings[["date"]].rename(columns={'date':'next_meeting'}),
            left_on='report_date', right_on='next_meeting',
            direction='forward'
        )

        df['prev_meeting'] = prev_merge['prev_meeting']
        df['next_meeting'] = next_merge['next_meeting']

        df = df[[datevar, "fomc_meeting", "prev_meeting", "next_meeting"]  + contentcols]

        return df



# %%
