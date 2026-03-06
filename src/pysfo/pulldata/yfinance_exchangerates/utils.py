#%% 

from datetime import date

class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

def add_fetch_stamps_to_filename(filename: str, **kwargs) -> str:

    downloaddate = kwargs.pop("downloaddate", None) 
    
    if downloaddate is None:
        today = date.today().strftime("%Y-%m-%d")
        fetch_stamps = f"downloaddate({today})"
    else :
        fetch_stamps = f"downloaddate({downloaddate})"

    for arg, val in kwargs.items():
        fetch_stamps += f"_{arg}({val})"

    return filename.format_map(SafeDict(fetch_stamps=fetch_stamps))
