import os
import logging
import requests
import datetime
from airflow import DAG
import pendulum
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
import xml.etree.ElementTree as ET

# Configurations
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "your-gcs-bucket-id")
BASE_URL = "https://cycling.data.tfl.gov.uk"  # Corrected Base URL
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'your_bigquery_dataset')
data_types = {
        "Wave": pd.StringDtype(),
        "SiteID": pd.StringDtype(),
        "Date": pd.StringDtype(),
        "Weather": pd.StringDtype(),
        "Time": pd.StringDtype(),
        "Day": pd.StringDtype(),
        "Round": pd.StringDtype(),
        "Direction": pd.StringDtype(),
        "Path": pd.StringDtype(),
        "Mode": pd.StringDtype(),
        "Count": pd.Int64Dtype()
}

def generate_cycleways_urls_from_xml():
    """
    Fetch and parse the XML to extract URLs for Cycleways.csv files.
    """
    XML_URL = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/"
    BASE_URL = "https://cycling.data.tfl.gov.uk/"

    # Fetch the XML file
    response = requests.get(XML_URL)
    response.raise_for_status()

    # Parse XML content
    root = ET.fromstring(response.content)
    namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    keys = root.findall("s3:Contents/s3:Key", namespace)

    # Filter for Cycleways.csv keys
    cycleways_keys = [key.text for key in keys if 'Cycleways.csv' in key.text]
    urls = [BASE_URL + key for key in cycleways_keys]

    # Log discovered URLs
    logging.info(f"Discovered URLs for Cycleways.csv: {urls}")

    return urls

def discover_files(**kwargs):
    """
    Discover file URLs for Cycleways.csv using the XML-based approach.
    """
    file_urls = generate_cycleways_urls_from_xml()
    if not file_urls:
        logging.warning("No Cycleways.csv files found in the XML.")
    else:
        logging.info(f"Cycleways.csv files discovered: {file_urls}")
    kwargs['ti'].xcom_push(key='file_urls', value=file_urls)


def download_file(file_url, local_path):
    """
    Download a file from a URL to a local path.
    """
    response = requests.get(file_url, stream=True)
    response.raise_for_status()
    with open(local_path, 'wb') as f:
        f.write(response.content)
    logging.info(f"Downloaded {file_url} to {local_path}")

def fix_malformed_csv(file_path, output_path, expected_columns=11):
    """
    Fix malformed rows in the CSV file by buffering incomplete rows,
    merging rows split across multiple lines, and skipping empty rows.
    """
    fixed_lines = []
    buffer = ""

    with open(file_path, 'r') as f:
        lines = f.readlines()

    for line_number, line in enumerate(lines, start=1):
        # Count the number of columns in the current line
        column_count = line.count(',') + 1

        # Skip rows that are completely empty
        if not line.strip():
            logging.warning(f"Skipping empty row at line {line_number}.")
            continue

        if column_count == expected_columns:
            # If current line is valid, check the buffer
            if buffer:
                buffer += line.strip()
                combined_column_count = buffer.count(',') + 1

                if combined_column_count == expected_columns:
                    fixed_lines.append(buffer.strip())
                    buffer = ""
                else:
                    logging.warning(f"Line {line_number} still malformed after merging: {buffer.strip()}")
            else:
                fixed_lines.append(line.strip())
        elif column_count < expected_columns:
            # Buffer the line for merging
            buffer += line.strip()
        elif column_count > expected_columns:
            # Log and skip rows with too many columns
            logging.warning(f"Line {line_number} has too many columns ({column_count}). Skipping: {line.strip()}")
            continue

    # Add the remaining buffer if valid
    if buffer:
        buffer_column_count = buffer.count(',') + 1
        if buffer_column_count == expected_columns:
            fixed_lines.append(buffer.strip())
        else:
            logging.warning(f"Buffer at end of file is malformed and skipped: {buffer.strip()}")

    # Write the fixed lines to the output file
    with open(output_path, 'w') as f:
        f.write('\n'.join(fixed_lines))

    # Log the number of rows in the fixed CSV
    logging.info(f"Number of rows in the fixed CSV ({output_path}): {len(fixed_lines)}")
    logging.info(f"Fixed malformed rows in {file_path} and saved to {output_path}")

def validate_and_clean_data(input_path, output_path, data_types):
    # Load the CSV file
    df = pd.read_csv(input_path, dtype=data_types)

    # Remove completely empty rows
    df = df.dropna(how='all')
    logging.info(f"Removed rows where all values are NaN. Remaining rows: {len(df)}")

    # Convert 'Date' column safely
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")  # Convert with error handling
    df["Time"] = pd.to_datetime(df["Time"], errors="coerce").dt.time

    # Identify invalid dates
    invalid_dates = df[df["Date"].isna()]
    if not invalid_dates.empty:
        logging.warning(f"Found {len(invalid_dates)} invalid dates. Replacing with NaT.")
    
    # Log rows with missing 'Path' or 'Count'
    missing_path_rows = df[df['Path'].isnull()]
    missing_count_rows = df[df['Count'].isnull()]

    logging.info(f"Rows with missing 'Path': {missing_path_rows.shape[0]}")
    logging.info(f"Rows with missing 'Count': {missing_count_rows.shape[0]}")

    # Save cleaned DataFrame
    df.to_csv(output_path, index=False)
    logging.info(f"Validated and cleaned data saved to {output_path}")
    logging.info(f"Number of rows in the cleaned CSV ({output_path}): {len(df)}")

def process_file(file_url, **kwargs):
    
    file_name = file_url.split('/')[-1]
    local_csv_path = f"{AIRFLOW_HOME}/{file_name}"
    fixed_csv_path = f"{AIRFLOW_HOME}/fixed_{file_name}"
    cleaned_csv_path = f"{AIRFLOW_HOME}/cleaned_{file_name}"
    local_parquet_path = cleaned_csv_path.replace('.csv', '.parquet')
   
    try:
        # Step 1: Download the file
        download_file(file_url, local_csv_path)

        # Step 2: Fix malformed rows and save the fixed file
        fix_malformed_csv(local_csv_path, fixed_csv_path)

        # Step 3: Validate and clean the data
        validate_and_clean_data(fixed_csv_path, cleaned_csv_path, data_types)
      
        # Step 4: Convert to Parquet
        df = pd.read_csv(cleaned_csv_path, dtype=data_types)

        # Ensure 'Date' is datetime.date and 'Time' is datetime.time
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df['Time'] = pd.to_datetime(df['Time']).dt.time

        # Define Parquet schema explicitly
        arrow_schema = pa.schema([
            ("Wave", pa.string()),
            ("SiteID", pa.string()),
            ("Date", pa.date32()),  # Correct Parquet DATE type
            ("Weather", pa.string()),
            ("Time", pa.time32("s")),  # Correct Parquet TIME type
            ("Day", pa.string()),
            ("Round", pa.string()),
            ("Direction", pa.string()),
            ("Path", pa.string()),
            ("Mode", pa.string()),
            ("Count", pa.int64()),
        ])

        # Convert DataFrame to Parquet with the defined schema
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
        pq.write_table(arrow_table, local_parquet_path)

        # Step 5: Upload to GCS
        client = storage.Client()
        bucket = client.bucket(BUCKET)
        blob = bucket.blob(f"raw/cycleways/{file_name.replace('.csv', '.parquet')}")
        blob.upload_from_filename(local_parquet_path)
        logging.info(f"Uploaded {local_parquet_path} to GCS as raw/cycleways/{file_name.replace('.csv', '.parquet')}")

    except Exception as e:
        logging.error(f"Failed to process {file_name}. Error: {str(e)}")
        raise
    
    # Step 6: Clean up local files
    finally: 
        for path in [local_csv_path, fixed_csv_path, cleaned_csv_path, local_parquet_path]:
            if os.path.exists(path):
                os.remove(path)
                logging.info(f"Removed local file: {path}")

def create_file_tasks(**kwargs):

    """
    Create dynamic tasks for each file URL discovered.
    """
    file_urls = kwargs['ti'].xcom_pull(key='file_urls', task_ids='discover_files_task')
    logging.info(f"Retrieved file URLs: {file_urls}")
    if not file_urls:
        raise ValueError("No file URLs discovered!")

    for file_url in file_urls:
        logging.info(f"Processing file URL: {file_url}")
        process_file(file_url)

# Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": pendulum.today('UTC').add(days=-1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_cycleways_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['tfl-cycles'],
) as dag:

    # Task 1: Discover files
    discover_files_task = PythonOperator(
        task_id="discover_files_task",
        python_callable=discover_files,
        provide_context=True,
    )

    # Task 2: Process files dynamically
    process_files_task = PythonOperator(
        task_id="process_files_task",
        python_callable=create_file_tasks,
        provide_context=True,
    )

    # Task 3: Create an external table in BigQuery
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "cycleways_external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/cycleways/*.parquet"],
                "schema": {
                    "fields": [
                        {"name": "Wave", "type": "STRING"},
                        {"name": "SiteID", "type": "STRING"},
                        {"name": "Date", "type": "DATE"},  # Explicitly specify as DATE
                        {"name": "Weather", "type": "STRING"},
                        {"name": "Time", "type": "TIME"},  # Explicitly specify as TIME
                        {"name": "Day", "type": "STRING"},
                        {"name": "Round", "type": "STRING"},
                        {"name": "Direction", "type": "STRING"},
                        {"name": "Path", "type": "STRING"},
                        {"name": "Mode", "type": "STRING"},
                        {"name": "Count", "type": "INT64"},
                    ]
                }
            },
        },
    )



    # Task Dependencies
    discover_files_task >> process_files_task >> bigquery_external_table_task

# Test Block (Optional)
if __name__ == "__main__":
    urls = generate_cycleways_urls_from_xml(BASE_URL)
    print("Discovered URLs:")
    print(urls)