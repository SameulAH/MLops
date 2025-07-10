# ingest_dag.py
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Ensure path to `include/ingest.py` is discoverable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from include.ingest import process_data, upload_to_minio

YELLOW_TRIPDATA = {
    "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
    "yellow_tripdata_2023-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
}

FOLDER = "raw"
AWS_CONN_ID = "minio_s3"

def make_ingest_task(url: str, filename: str):
    def task_callable():
        df = process_data(url)
        upload_to_minio(df, filename, folder=FOLDER, aws_conn_id=AWS_CONN_ID)
    return task_callable

with DAG(
    dag_id="ingest_yellow_tripdata_minio",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["yellow-taxi", "minio", "mlops"],
) as dag:

    for filename, url in YELLOW_TRIPDATA.items():
        PythonOperator(
            task_id=f"ingest_{filename.replace('.parquet', '')}",
            python_callable=make_ingest_task(url, filename)
        )
