# # dags/full_training_pipeline.py
import sys
import os
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from mlflow_provider.hooks.client import MLflowClientHook

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from include.ingest import process_data, upload_to_minio
from include.preprocessing import run_preprocessing
from include.training import run_train

# Constants
YELLOW_TRIPDATA = {
    "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
    "yellow_tripdata_2023-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
}
FOLDER = "raw"
AWS_CONN_ID = "minio_s3"
MLFLOW_CONN_ID = "mlflowconn"
MINIO_CONN_ID = "minio_s3"
EXPERIMENT_NAME = "rf_experiment4"
REGISTERED_MODEL_NAME = "RF-Model"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="full_training_pipeline",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    tags=["yellow-taxi", "mlops", "training", "minio"],
)
def full_training_pipeline():

    # Create dynamic ingestion tasks using classic PythonOperator
    def make_ingest_task(url: str, filename: str):
        def task_callable():
            df = process_data(url)
            upload_to_minio(df, filename, folder=FOLDER, aws_conn_id=AWS_CONN_ID)
        return task_callable

    ingest_tasks = []
    for filename, url in YELLOW_TRIPDATA.items():
        ingest_task = PythonOperator(
            task_id=f"ingest_{filename.replace('.parquet', '')}",
            python_callable=make_ingest_task(url, filename),
        )
        ingest_tasks.append(ingest_task)

    # PREPROCESSING (also a classic function wrapped by PythonOperator)
    preprocess_task = PythonOperator(
        task_id="run_preprocessing",
        python_callable=run_preprocessing,
    )

    # Functional API tasks
    @task()
    def setup_minio():
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3_client = s3_hook.get_conn()
        response = s3_client.list_buckets()
        print(f"Connected to MinIO. Buckets: {[bucket['Name'] for bucket in response['Buckets']]}")

    @task()
    def create_experiment_if_not_exists():
        hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
        try:
            experiments_info = hook.run(
                endpoint="api/2.0/mlflow/experiments/search",
                request_params={"max_results": 1000}
            ).json()
        except Exception as e:
            raise AirflowFailException(f"Failed to reach MLflow API: {e}")
        for experiment in experiments_info.get("experiments", []):
            if experiment["name"] == EXPERIMENT_NAME:
                return experiment["experiment_id"]
        response = hook.run(
            endpoint="api/2.0/mlflow/experiments/create",
            method="POST",
            request_json={"name": EXPERIMENT_NAME}
        ).json()
        return response["experiment_id"]

    @task(execution_timeout=timedelta(minutes=30), retries=1, retry_delay=timedelta(minutes=10))
    def train_model_task():
        run_train()

    @task()
    def register_model():
        hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
        response = hook.run(
            endpoint="api/2.0/mlflow/registered-models/create",
            request_params={"name": REGISTERED_MODEL_NAME}
        )
        print(f"Registered model response: {json.dumps(response.json(), indent=2)}")

    # Task dependencies
    for t in ingest_tasks:
        t >> preprocess_task

    setup_task = setup_minio()
    create_exp_task = create_experiment_if_not_exists()
    train_task = train_model_task()
    register_task = register_model()

    preprocess_task >> setup_task >> create_exp_task >> train_task >> register_task

full_training_pipeline()

























# import sys
# import os
# import json
# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.decorators import dag, task
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.hooks.base import BaseHook
# from mlflow_provider.hooks.client import MLflowClientHook
# from airflow.exceptions import AirflowFailException

# # Ensure access to local modules
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from include.ingest import process_data, upload_to_minio
# from include.preprocessing import run_preprocessing
# from include.training import run_train

# # Constants
# YELLOW_TRIPDATA = {
#     "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
#     "yellow_tripdata_2023-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
#     "yellow_tripdata_2023-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
# }
# FOLDER = "raw"
# AWS_CONN_ID = "minio_s3"
# MLFLOW_CONN_ID = "mlflowconn"
# MINIO_CONN_ID = "minio_s3"
# EXPERIMENT_NAME = "rf_experiment3"
# REGISTERED_MODEL_NAME = "RF-Model"

# default_args = {
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# @dag(
#     dag_id="full_training_pipeline",
#     schedule_interval="@once",
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     default_args=default_args,
#     dagrun_timeout=timedelta(hours=2),
#     tags=["yellow-taxi", "mlops", "training", "minio"],
# )
# def full_training_pipeline():

#     # INGESTION
#     def make_ingest_task(url: str, filename: str):
#         def task_callable():
#             df = process_data(url)
#             upload_to_minio(df, filename, folder=FOLDER, aws_conn_id=AWS_CONN_ID)
#         return task_callable

#     ingest_tasks = []
#     for filename, url in YELLOW_TRIPDATA.items():
#         task = PythonOperator(
#             task_id=f"ingest_{filename.replace('.parquet', '')}",
#             python_callable=make_ingest_task(url, filename)
#         )
#         ingest_tasks.append(task)

#     # PREPROCESSING
#     preprocess_task = PythonOperator(
#         task_id="run_preprocessing",
#         python_callable=run_preprocessing,
#     )

#     # TRAINING
#     @task
#     def setup_minio():
#         s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
#         s3_client = s3_hook.get_conn()
#         response = s3_client.list_buckets()
#         print(f"Connected to MinIO. Buckets: {[bucket['Name'] for bucket in response['Buckets']]}")

#     @task
#     def create_experiment_if_not_exists():
#         hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
#         try:
#             experiments_info = hook.run(
#                 endpoint="api/2.0/mlflow/experiments/search",
#                 request_params={"max_results": 1000}
#             ).json()
#         except Exception as e:
#             raise AirflowFailException(f"Failed to reach MLflow API: {e}")
#         for experiment in experiments_info.get("experiments", []):
#             if experiment["name"] == EXPERIMENT_NAME:
#                 return experiment["experiment_id"]
#         response = hook.run(
#             endpoint="api/2.0/mlflow/experiments/create",
#             method="POST",
#             request_json={"name": EXPERIMENT_NAME}
#         ).json()
#         return response["experiment_id"]

#     @task(execution_timeout=timedelta(minutes=30), retries=1, retry_delay=timedelta(minutes=10))
#     def train_model_task():
#         run_train()

#     @task
#     def register_model():
#         hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)
#         response = hook.run(
#             endpoint="api/2.0/mlflow/registered-models/create",
#             request_params={"name": REGISTERED_MODEL_NAME}
#         )
#         print(f"Registered model response: {json.dumps(response.json(), indent=2)}")

#     # TASK CHAINS
#     setup_minio_task = setup_minio()
#     experiment_task = create_experiment_if_not_exists()
#     train_task = train_model_task()
#     register_task = register_model()

#     for task in ingest_tasks:
#         task >> preprocess_task

#     preprocess_task >> setup_minio_task >> experiment_task >> train_task >> register_task

# full_training_pipeline()
