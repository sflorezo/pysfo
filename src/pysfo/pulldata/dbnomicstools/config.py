def load_json(file_path):

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


def extract_indicators_from_json(json_path):

    import pandas as pd

    metadata = load_json(json_path)


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

__all__ = [
    "extract_indicators_from_json"
]

# CLEAN: Old version of the function
# def extract_indicators_from_json(json_path):

#     import pandas as pd

#     metadata = load_json(json_path)
    
#     code_lists = metadata.get('Structure', {}).get('CodeLists', {}).get('CodeList', [])
#     ids = [code.get('@id') for code in code_lists]
#     codes = [code.get('Code') for code in code_lists]
#     names = [code.get('Name') for code in code_lists]

#     df_list = []

#     for id_, code, name in zip(ids, codes, names):
        
#         elems = [
#             [
#                 id_,
#                 name["#text"],
#                 el["@value"], 
#                 el["Description"]["#text"]
#             ] for el in code
#         ]

#         df = pd.DataFrame(
#             data = elems,
#             columns = ["ID", "NAME", "VALUE", "DESCRIPTION_TEXT"]
#         )

#         df_list.append(df)

#     df = pd.concat(df_list, axis = 0)

#     return df
