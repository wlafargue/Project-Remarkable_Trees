from airflow.decorators import task
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

# Configurations
kaggle_credentials = '../config/kaggle.json'

@task
def extract(dataset_name, csv_file, downloads_path):
    """ Download dataset from Kaggle using the Kaggle API and store 
        data into 'downloads_path' directory.

    Args:
        dataset_name (str): Kaggle API key
    
    """

    print('Start extracting data...')

    # Initialize the Kaggle API
    api = KaggleApi(api_key_path=kaggle_credentials)

    # Download files
    api.dataset_download_files(dataset_name, 
                               path=downloads_path, 
                               unzip=True)
    
    df = pd.read_csv('{downloads_path}/{csv_file}', sep=';')

    print('Data successfully extracted!')

    return df

