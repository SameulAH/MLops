# dags/ingest_to_minio_dag.py
import sys
import os
from airflow.decorators import dag, task
from datetime import datetime


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingest import upload_to_minio

default_args = {
    "owner": "airflow1",
    "start_date": datetime(2024, 1, 1),
    "retries": 2
}

@dag(
    dag_id="yellow_trip_ingestminio1",
    default_args=default_args,
    description="Download Yellow Taxi data to store in MinIO",
    schedule_interval=None,
    catchup=False,
    tags=["yellow", "minioingest", "ingestion"]
)
def yellow_ingest_dag():

    @task()
    def ingest_dataset(url: str, filename: str):
        upload_to_minio(url, filename, folder="raw")

    urls = {
        "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
        "yellow_tripdata_2023-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
        "yellow_tripdata_2023-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
    }

    for filename, url in urls.items():
        ingest_dataset.override(task_id=f"ingest_{filename.replace('.parquet', '')}")(url, filename)

dag_instance = yellow_ingest_dag()