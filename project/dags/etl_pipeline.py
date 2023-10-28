
from datetime import datetime

from . import extract, transform, load

from airflow import DAG

# Dataset
dataset_name = 'mpwolke/cusersmarildownloadstreescsv'
csv_file = 'trees.csv'
downloads_path = '../data/raw'

# Postgres database
db_params = {
    'database': 'remarkable_trees',
    'host': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5433'
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
