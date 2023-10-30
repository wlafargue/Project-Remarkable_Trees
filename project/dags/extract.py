import json
from airflow.decorators import task
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

@task
def extract(dataset_name, csv_file, downloads_path):
    """ Download dataset from Kaggle using the Kaggle API and store 
        data into 'downloads_path' directory.

    Args:
        dataset_name (str): Kaggle API key
        csv_file (str): CSV file
        downloads_path (str): path to downloads directory
    
    """

    print('Start extracting data...')

    # Initialize the Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download files
    api.dataset_download_file(dataset_name, file_name=f'{csv_file}', path=downloads_path)
    
    df = pd.read_csv(f'./data/{csv_file}', sep=';')

    print('Data successfully extracted!')

    return df

