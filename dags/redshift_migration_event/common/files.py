import logging
import os
import json
import pyarrow.parquet as pq
from google.cloud import storage

log = logging.getLogger()

dags_folder = os.getenv('DAGS_FOLDER')


def load_schema_from_json(json_file_path):
    full_file_path = '%s/%s' % (dags_folder, json_file_path)
    with open(full_file_path) as f:
        schema_json = json.load(f)
    return schema_json


def read_sql_file(file_path):
    full_file_path = '%s/%s' % (dags_folder, file_path)
    with open(full_file_path, 'r') as file:
        sql_file = file.read()
    log.info(f"Unload SQL: {sql_file}")
    return sql_file


def calculate_total_rows(bucket_name, prefix):
    # Establish a client for interacting with GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    total_rows = 0

    log.info(f"Starting validation of total rows at {bucket_name}/{prefix}")
    # List all the parquet files in the GCS path
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    # For each file
    for blob in blobs:
        # Download the file to a local path
        file_name = os.path.basename(blob.name)
        blob.download_to_filename(file_name)
        # Open the parquet file
        parquet_file = pq.ParquetFile(file_name)
        # Get the number of rows
        num_rows = parquet_file.metadata.num_rows
        log.info(f"File {file_name} has {num_rows} rows")
        total_rows += num_rows
        # Delete the local file
        os.remove(file_name)

    log.info(f"Total rows: {total_rows}")
    return total_rows
