
#%% implemented

def get_filters(filter = None):

    import os
    # from pysfo.dbnomicstools.config import extract_indicators_from_json
    from ..config import extract_indicators_from_json

    current_dir = os.path.dirname(__file__)
    json_path = os.path.join(current_dir, "dbnomics_datastructure.json")

    metadata = extract_indicators_from_json(json_path)

    if filter is None:
        return "Please specify a filter of ['INDICATOR', 'REF_AREA', 'FREQ']"
    elif filter == "INDICATOR":
        return metadata.loc[metadata["ID"] == "INDICATOR", :]
    elif filter == "REF_AREA":
        return metadata.loc[metadata["ID"] == "REF_AREA", :]
    elif filter == "FREQ":
        return metadata.loc[metadata["ID"] == "FREQ", :]
    else:
        return "Filter not implemented yet."

