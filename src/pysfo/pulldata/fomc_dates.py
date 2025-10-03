#%%

def get_fomc_meeting_dates(FOMC_meeting_dates_path) :

    import pandas as pd
    import os
    
    # Construct the full path to the CSV file
    file_path = f"{FOMC_meeting_dates_path}/fomc_meeting_dates_2018_onward.csv"
    
    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The required file '{file_path}' is missing.")

    meetings = pd.read_csv(f"{FOMC_meeting_dates_path}/fomc_meeting_dates_2018_onward.csv") 
    meetings.columns = meetings.columns.str.lower()
    meetings["date"] = pd.to_datetime(meetings["date"])
    return meetings

def merge_fomc_meeting_dates(df, datevar, FOMC_meeting_dates_path):

    import numpy as np
    import pandas as pd
    
    print("Merging FOMC meeting dates to closest meeting, both before and after 'datevar'")

    meetings = get_fomc_meeting_dates(FOMC_meeting_dates_path)
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
