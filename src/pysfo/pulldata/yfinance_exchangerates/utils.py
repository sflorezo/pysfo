#%% 

from datetime import date

class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"

def add_fetch_stamps_to_filename(filename: str, **kwargs) -> str:
    today = date.today().strftime("%Y-%m-%d")
    fetch_stamps = f"downloaddate({today})"
    for arg, val in kwargs.items():
        fetch_stamps += f"_{arg}({val})"

    return filename.format_map(SafeDict(fetch_stamps=fetch_stamps))
