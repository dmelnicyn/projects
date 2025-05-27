import os
import logging
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pandas as pd

# Configurations
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "your-gcs-bucket-id")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'your_bigquery_dataset')

DATASET_FILE = "1%20Monitoring%20locations.csv"
DATASET_URL = f"https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/{DATASET_FILE}"
PARQUET_FILE = DATASET_FILE.replace('.csv', '.parquet')

DATA_TYPES = {
    'Site ID': pd.StringDtype(),
    'Location description': pd.StringDtype(),
    'Borough': pd.StringDtype(),
    'Functional area for monitoring': pd.StringDtype(),
    'Road type': pd.StringDtype(),
    'Is it on the strategic CIO panel?': pd.Int64Dtype(),
    'Old site ID (legacy)': pd.StringDtype(),
    'Easting (UK Grid)': float,
    'Northing (UK Grid)': float,
    'Latitude': float,
    'Longitude': float,
}


# Functions
def download_dataset(local_file, dataset_url):
    """
    Download the dataset from the given URL and save it locally.
    """
    try:
        logging.info(f"Downloading dataset from {dataset_url} to {local_file}")
        os.system(f"curl -sSL {dataset_url} > {local_file}")
    except Exception as e:
        logging.error(f"Failed to download dataset. Error: {str(e)}")
        raise


def format_to_parquet(src_file, parquet_file):
    """
    Convert the CSV file to Parquet format, clean, and deduplicate.
    """
    if not src_file.endswith('.csv'):
        logging.error("Source file must be in CSV format.")
        raise ValueError("Invalid source file format.")
    
    try:
        logging.info(f"Reading and formatting {src_file} to {parquet_file}")
        df = pd.read_csv(src_file, dtype=DATA_TYPES)
        
        # Drop completely empty rows
        initial_rows = len(df)
        df = df.dropna(how='all')
        logging.info(f"Removed {initial_rows - len(df)} empty rows. Remaining: {len(df)}")

        # Deduplicate based on 'Site_ID'
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['Site ID'], keep='first')
        logging.info(f"Removed {initial_rows - len(df)} duplicate rows. Remaining: {len(df)}")
        
        # Save as Parquet
        df.to_parquet(parquet_file, index=False)
        logging.info(f"Saved Parquet file to {parquet_file}")
    except Exception as e:
        logging.error(f"Failed to format {src_file} to Parquet. Error: {str(e)}")
        raise


def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Upload a local file to Google Cloud Storage.
    """
    try:
        logging.info(f"Uploading {local_file} to GCS bucket {bucket_name} as {object_name}")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)
        logging.info(f"File {local_file} uploaded successfully.")
    except Exception as e:
        logging.error(f"Failed to upload {local_file} to GCS. Error: {str(e)}")
        raise


# DAG Definition
default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').add(days=-1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_monitoring_locations_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['tfl-cycles'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download_dataset,
        op_kwargs={
            "local_file": f"{AIRFLOW_HOME}/{DATASET_FILE}",
            "dataset_url": DATASET_URL,
        },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{DATASET_FILE}",
            "parquet_file": f"{AIRFLOW_HOME}/{PARQUET_FILE}",
        },
    )

    upload_to_gcs_task = PythonOperator(
        task_id="upload_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "object_name": f"raw/monitoring_locations/{PARQUET_FILE}",
            "local_file": f"{AIRFLOW_HOME}/{PARQUET_FILE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "monitoring_locations_external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/monitoring_locations/{PARQUET_FILE}"],
            },
        },
    )

    # Task Dependencies
    download_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> bigquery_external_table_task
