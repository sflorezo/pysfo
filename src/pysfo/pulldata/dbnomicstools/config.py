def _load_json(file_path):

    import json

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing JSON file: {file_path}")
        return None

def _extract_filters_from_json(json_path):

    import pandas as pd

    metadata = _load_json(json_path)


    dim_codes = metadata["dimensions_codes_order"]
    dim_labels = metadata["dimensions_labels"]
    dim_values = metadata["dimensions_values_labels"]

    df_list = []

    for dim in dim_codes:
        
        elems = [
            [
                dim, 
                dim_labels[dim],
                key,
                val
            ] for key, val in dim_values[dim].items()
        ]

        df = pd.DataFrame(
            data = elems,
            columns = ["ID", "NAME", "VALUE", "DESCRIPTION_TEXT"]
        )

        df_list.append(df)

    df = pd.concat(df_list, axis = 0)

    return df

def get_filters(json_metadata_path, filter = None):

    metadata = _extract_filters_from_json(json_metadata_path)

    if filter is not None:
        return metadata.loc[metadata["ID"] == f"{filter}", :]

    return metadata
    
__all__ = [
    "get_filters"
]

