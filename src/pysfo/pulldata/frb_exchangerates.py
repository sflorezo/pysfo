#%%========== define helper functions ==========%%#

def parse_er_xml(file_path):

    import xml.etree.ElementTree as ET

    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Namespace dictionary for prefix resolution
    namespaces = {
        'kf': 'http://www.federalreserve.gov/structure/compact/H10_H10',
        'frb': 'http://www.federalreserve.gov/structure/compact/common',
        'common': 'http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common'
    }
    
    # Initialize dictionary for datasets
    datasets = {}

    # Iterate over each kf:Series element
    for series in root.findall('.//kf:Series', namespaces):
        # Extract attributes
        currency = series.attrib.get('CURRENCY', 'NaN')
        frequency = series.attrib.get('FREQ', 'NaN')
        fx = series.attrib.get('FX', 'NaN')
        series_name = series.attrib.get('SERIES_NAME', 'NaN')
        unit = series.attrib.get('UNIT', 'NaN')
        unit_mult = series.attrib.get('UNIT_MULT', 'NaN')
        
        # Add to datasets dictionary
        datasets[series_name] = {
            'currency': currency,
            'frequency': frequency,
            'fx': fx,
            'unit': unit,
            'unit_mult': unit_mult,
            'observations': []
        }

        # Extract long description from annotations
        annotations = series.find('.//frb:Annotations', namespaces)
        if annotations is not None:
            for annotation in annotations.findall('.//common:Annotation', namespaces):
                annotation_type = annotation.find('common:AnnotationType', namespaces).text
                if annotation_type == 'Long Description':
                    long_desc = annotation.find('common:AnnotationText', namespaces).text
                    datasets[series_name]['long_description'] = long_desc


        # Extract each observation
        for obs in series.findall('./frb:Obs', namespaces):
            obs_status = obs.attrib.get('OBS_STATUS', 'NaN')
            obs_value = obs.attrib.get('OBS_VALUE', 'NaN')
            time_period = obs.attrib.get('TIME_PERIOD', 'NaN')
            
            datasets[series_name]['observations'].append({
                'status': obs_status,
                'value': obs_value,
                'time_period': time_period
            })

    return datasets

def create_er_dataframe(er_data_dict):

    import pandas as pd

    # Initialize a list to collect data for each row
    rows = []

    for series_name, series_data in er_data_dict.items():
        # Iterate over observations
        for obs in series_data['observations']:
            # Each observation is a row in the DataFrame
            row = {
                'series_name': series_name,
                'currency': series_data['currency'],
                'frequency': series_data['frequency'],
                'fx': series_data['fx'],
                'unit': series_data['unit'],
                'unit_mult': series_data['unit_mult'],
                'long_description': series_data['long_description'],
                'obs_status': obs['status'],
                'obs_value': obs['value'],
                'time_period': obs['time_period']
            }
            rows.append(row)

    # Create DataFrame from list of dictionaries
    df = pd.DataFrame(rows)
    return df

def parse_datadict_xml(frequencies_file):

    import xml.etree.ElementTree as ET

     # Parse the XML file
    tree = ET.parse(frequencies_file)
    root = tree.getroot()

    # Namespace dictionary for prefix resolution
    namespaces = {
        'xs': 'http://www.w3.org/2001/XMLSchema',
    }
    
    # Initialize dictionary for datasets
    datasets = {}

    # Iterate over each kf:Series element
    for data in root.findall('.//xs:simpleType', namespaces):
        # Extract attributes
        name = data.attrib.get('name', 'NaN')
        datasets[name] = {}

        # Extract each observation
        for item, docum in zip(data.findall('.//xs:enumeration', namespaces),
                               data.findall('.//xs:documentation', namespaces)):
            
            value = item.attrib.get('value', 'NaN')
            desc = docum.text
            
            datasets[name][value] = desc

    return datasets

def create_datadict_dataframe(datadict_dict):

    import pandas as pd

    # Initialize a list to collect data for each row
    rows = []

    for datapoint, values in datadict_dict.items():

        # Iterate over values
        for val, desc in values.items():
            # Each observation is a row in the DataFrame
            row = {
                'datapoint': datapoint,
                'value': val,
                'description': desc,
            }
            rows.append(row)

    # Create DataFrame from list of dictionaries
    df = pd.DataFrame(rows)
    return df

#%%========== get data ==========%%#


class FRBExchangeRates:

    @staticmethod
    def about():

        return (
            "Daily Foreign Exchange Rates, USD vs. Important Currencies, retrieved from the FRB H.10 Tables."
        )

    @staticmethod
    def print_instructions():
        
        from .config import get_data_path

        FRB_H10_dir = get_data_path() / "FRB_H10/FRB_H10"

        return (
            f"To use this dataset, download the H.10 Daily rates zip pacakge from the FRB/Federal Reserve Webpage and unzip the retrieved file. The final target directory should have the followig files:\n\n"
            'frb_common.xsd\n' 
            'H10_data.xml\n' 
            'H10_H10.xsd\n'
            'H10_struct.xml\n\n'
            f"which should be stored in the following directory {FRB_H10_dir}\n"
        )
    
    @staticmethod
    def get(check_ER = False):

        import os
        import pandas as pd
        from .config import get_data_path

        print("Exchange rates extracted from FRB H10 table.\n")

        FRB_H10_dir = get_data_path() / "FRB_H10/FRB_H10"

        # Define file paths
        file_path = f'{FRB_H10_dir}/H10_data.xml'
        datadict_path = f'{FRB_H10_dir}/frb_common.xsd'

        # Check if files exist
        for path in [file_path, datadict_path]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"The required file '{path}' is missing.")
        
        # Get data
        datasets = parse_er_xml(file_path)
        df = create_er_dataframe(datasets)

        datadict = parse_datadict_xml(datadict_path)
        datadict = create_datadict_dataframe(datadict)

        # fix general formats

        df[["frequency", "obs_value"]] = df[["frequency", "obs_value"]].apply(lambda x : pd.to_numeric(x))
        df["time_period"] = pd.to_datetime(df["time_period"]) 

        # keep business day ER and normal

        df = df[df["frequency"] == 9]
        df = df[df["obs_status"] == "A"]

        # fix ER names (everything looks kind of nice)

        # Note: This is for checks
        # statatab(df["series_name"])
        
        h_tmp = (
            df
            .groupby(["long_description", "series_name", "frequency", "unit", "obs_status"])
            .agg(max_date = ("time_period", "max"),
                min_date = ("time_period", "min"),
                nobs = ("obs_value", "count"))
            .reset_index()
            .drop_duplicates()
            .sort_values(by = "long_description")
        )

        if check_ER:
            h_tmp.to_csv(f"{FRB_H10_dir}/check.csv")

        # take out series no longer updated

        mask = df["series_name"].isin(["V0.JRXWTFB_N.B", "V0.JRXWTFN_N.B", "V0.JRXWTFO_N.B"])
        df = df.loc[~mask, :]

        # save fx data

        return df

# %%
