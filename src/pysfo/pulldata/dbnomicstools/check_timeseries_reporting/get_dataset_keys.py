def get_dataset_keys(provider, dataset):

    import pysfo.pulldata.imf_bop as imf_bop_root
    import pysfo.pulldata.imf_ifs as imf_ifs_root
    from pathlib import Path
    import os
    import json

    if (
        (provider == "IMF" and dataset == "BOP")
    ):
        
        series_key = "indicator"
        period_key = "period"
        country_key = "ref_area"
        country_key_iso = "ISO2"
        country_key_desc = "ref_area_desc"
        freq_key = "freq"
        metadata_path = os.path.dirname(Path(imf_bop_root.__file__))
        get_series_fn = imf_bop_root.imfBOP.get
        

    elif (
        (provider == "IMF" and dataset == "IFS")
    ):
        
        series_key = "indicator"
        period_key = "period"
        country_key = "ref_area"
        country_key_iso = "ISO2"
        country_key_desc = "ref_area_desc"
        freq_key = "freq"
        metadata_path = os.path.dirname(Path(imf_ifs_root.__file__))
        get_series_fn = imf_ifs_root.imfIFS.get

    else :
        
        raise ValueError(f"Reporting Check not yet implemented for provider = {provider}, dataset = {dataset}")
    
    with open(Path(os.path.dirname(__file__)) / "ignore_countries.json") as f:
            ignore_countries = json.load(f).get(provider, {}).get(dataset, [])

    return (
        series_key, 
        period_key, 
        country_key, 
        country_key_iso, 
        country_key_desc, 
        freq_key, 
        metadata_path, 
        ignore_countries,
        get_series_fn
    )