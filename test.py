from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import requests
import json
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
import pandas as pd



mysql_connection = {
    'host' : 'localhost',
    'user' : 'root',
    'password' : 'samio.db',
    'database' : 'worldair'
}

query = text("SELECT coordinates_latitude, coordinates_longitude, parameter, value from measurements")
engine = create_engine("mysql+mysqlconnector://{user}:{password}@{host}/{database}".format(**mysql_connection))

# Replace these values with your own
project_id = 'gentle-post-410810'
dataset_id = 'worldair'
table_id = 'measurements'  # Define the table_id here

client = bigquery.Client(project=project_id)

dataset_ref = client.dataset(dataset_id, project=project_id)
try:
    dataset = client.get_dataset(dataset_ref)
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)

# Create a table reference
table_ref = dataset_ref.table(table_id)

try:
    table = client.get_table(table_ref)
    print(f'Table {project_id}.{dataset_id}.{table_id} already exists.')
except NotFound:
    # Table doesn't exist, create it with the desired schema
    schema = [
        bigquery.SchemaField('coordinates_latitude', 'FLOAT'),
        bigquery.SchemaField('coordinates_longitude', 'FLOAT'),
        bigquery.SchemaField('parameter', 'STRING'),
        bigquery.SchemaField('value', 'FLOAT'),
        # Add more fields as needed
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f'Table {project_id}.{dataset_id}.{table_id} created with schema.')

# Connection to MySQL
chunk_size = 10000  # Adjust the batch size as needed
#initialize BQ client
client = bigquery.Client(project=project_id)
# Iterate through chunks
for chunk_df in pd.read_sql_query(query, engine, chunksize=chunk_size):
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(chunk_df, table_ref, job_config=job_config)
    job.result()

print(f'Data uploaded to BigQuery table {project_id}.{dataset_id}.{table_id}')