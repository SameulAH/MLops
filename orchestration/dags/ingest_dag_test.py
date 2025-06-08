# # dags/ingest_to_minio_dag.py
# import sys
# import os
# from airflow.decorators import dag, task
# from datetime import datetime


# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# from ingest import upload_to_minio

# default_args = {
#     "owner": "airflow1",
#     "start_date": datetime(2024, 1, 1),
#     "retries": 2
# }

# @dag(
#     dag_id="yellow_trip_ingestminio1",
#     default_args=default_args,
#     description="Download Yellow Taxi data to store in MinIO",
#     schedule_interval=None,
#     catchup=False,
#     tags=["yellow", "minioingest", "ingestion"]
# )
# def yellow_ingest_dag():

#     @task()
#     def ingest_dataset(url: str, filename: str):
#         upload_to_minio(url, filename, folder="raw")

#     urls = {
#         "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
#         "yellow_tripdata_2023-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
#         "yellow_tripdata_2023-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
#     }

#     for filename, url in urls.items():
#         ingest_dataset.override(task_id=f"ingest_{filename.replace('.parquet', '')}")(url, filename)

# dag_instance = yellow_ingest_dag()

import sys
import os   
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from include.ingest_test import process_data, upload_to_minio   

# Constants
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
FILENAME = "yellow_tripdata_2023-01.parquet"
FOLDER = "raw"
AWS_CONN_ID = "minio_s3"

def ingest_task():
    df = process_data(URL)
    upload_to_minio(df, FILENAME, folder=FOLDER, aws_conn_id=AWS_CONN_ID)

with DAG(
    dag_id="inges_minio_fulldata",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["mlops", "minio"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_and_upload_to_minio",
        python_callable=ingest_task
    )

    ingest
