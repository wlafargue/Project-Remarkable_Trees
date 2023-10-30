
from datetime import datetime
import os

from extract import extract
from load import load
from transform import transform

from airflow import DAG

# Kaggle
dataset_name = os.environ.get('KAGGLE_DATASET_PATH')
csv_file = os.environ.get('KAGGLE_DATASET_CSV')
downloads_path = os.environ.get('DOWNLOADS')

# Postgres
db_params = {
    'POSTGRES_DB': os.environ.get('POSTGRES_DB'),
    'POSTGRES_USER': os.environ.get('POSTGRES_USER'),
    'POSTGRES_PASSWORD': os.environ.get('POSTGRES_PASSWORD'),
    'POSTGRES_HOST': os.environ.get('POSTGRES_HOST'),
    'POSTGRES_PORT': os.environ.get('POSTGRES_PORT')
}

with DAG(dag_id='etl_pipeline',
         description='A simple ETL pipeline using Python, PostgreSQL, Apache Airflow and Docker',
         schedule=None,
         start_date=datetime(year=2023, month=10, day=25),
         catchup=False,
         tags=['etl_pipeline']) as dag:
    extracted_data = extract(dataset_name, csv_file, downloads_path)
    transformed_data = transform(extracted_data)
    load(transformed_data, db_params)
