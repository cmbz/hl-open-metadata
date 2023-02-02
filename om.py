"""
Harvard Open Metadata Project
"""

import pandas as pd

def create_datafile_metadata(inventory_df, template):
    """
    Create metadata for open metadata project datafiles based upon a template

    Parameters
    ----------
    inventory_df : DataFrame
        DataFrame containing list of datafiles to upload

    template : str
        String used to generate metadata to be applied to each datafile in the inventory

    Raise
    -----
    KeyError
        Inventory is missing required fields

    Return
    -------
    DataFrame
    """

    # validate parameters
    if ((inventory_df.empty == True) or
        (not template)):
        return pd.DataFrame()

    # check the dataframe for required fields
    if ((not 'file_part' in inventory_df.columns) or
        (not 'filename' in inventory_df.columns)):
        raise KeyError('Inventory is missing required fields')

    # copy the inventory
    df = inventory_df.copy(deep=True)

    # Prepare metadata for each of the inventory data files
    descriptions = []

    # Get the number of rows in the CSV
    num_files = len(df)

    # Loop through through the file list and assemblage metadata
    for row in df.iterrows():
        part = row[1].get('file_part')
        filename = row[1].get('filename')
        desc = template + ' File part: {} of {}'.format(part, num_files)
        descriptions.append(desc)

    # Add columns to the dataframe
    df['description'] = pd.Series(descriptions)

    return df

def direct_upload_datafiles(api, dataverse_url, dataset_pid, data_directory, metadata_df):
    """
    Upload Open Metadata datafiles to dataverse repository using direct upload method

    Parameters
    ----------
    api : pyDataverse api
    dataverse_url : str
        Dataverse installation
    dataset_pid : str
        Persistent identifier for the dataset (its DOI, takes form: doi:xxxxx)
    data_directory : str
        Directory where datafiles are kept
    metadata_df : DataFrame
        DataFrame containing metadata about Open Metadata datafiles to upload

    Return
    ------
        (bool, list, bool) (upload success, error list, finalize success)
            True, [], True
            False, [List of error messages], True
    """
    # validate paramters
    if ((not api) or
        (not dataverse_url) or
        (not dataset_pid) or
        (metadata_df.empty == True)):
        return False
    
    # error messages
    errors = []

    # per file json data array
    json_data = []
    categories = ['Data']

    # upload each datafile in the metadata dataframe
    import ddu # local module
    key = api.api_token
    for row in metadata_df.iterrows():
        filename = row[1].get('filename')
        description = row[1].get('description')
        mime_type = 'application/zip'

         # upload the datafile
        data = {}
        data = ddu.direct_upload(dataverse_url, dataset_pid, key, filename, data_directory, mime_type, retries=10)
        if (data == None):
            msg ='Warning: Failed to upload: {}'.format(filename)
            errors.append(msg)
        else:
            data['description'] = description
            data['categories'] = categories
            json_data.append(data)

    # finalize the direct upload
    status = ddu.finalize_direct_upload(dataverse_url, dataset_pid, json_data, key)

    # return errors, if any
    if (len(errors) > 0):
        return (False, errors, status)
    else:
        return (True, [], status)
