from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from mlflow_provider.hooks.client import MLflowClientHook
from pendulum import datetime
import time
from airflow.exceptions import AirflowFailException
from datetime import timedelta
import os
import sys
import json

# Custom module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from include.training import run_train  # your model training script

# Constants
MLFLOW_CONN_ID = "mlflowconn"
MINIO_CONN_ID = "minio_s3"
EXPERIMENT_NAME = "rf_experiment2"
ARTIFACT_BUCKET = "mlflowdata"
REGISTERED_MODEL_NAME = "my_model"
max_results=100

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
@dag(
    schedule=None,
    start_date=datetime(2025, 6, 8),
    dagrun_timeout=timedelta(hours=1),
    catchup=False,
    tags=["mlflow", "training"],
)
def mlflow_training_dag():

    @task
    def setup_minio():
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3_client = s3_hook.get_conn()
        response = s3_client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        print(f"Connected to MinIO. Buckets: {buckets}")

    @task
    def create_experiment_if_not_exists():
        """Use MLflowClientHook to check/create the experiment."""
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

        # If experiment does not exist, create it
        response = hook.run(
            endpoint="api/2.0/mlflow/experiments/create",
            method="POST",
            request_json={"name": EXPERIMENT_NAME}
        ).json()

        return response["experiment_id"]
    @task(execution_timeout=timedelta(minutes=10),  # Cap training runtime
        retries=1,
        retry_delay=timedelta(minutes=10))
    def train_model_task():
        for i in range(66):  # emit something every 60s
            print(f"Training step {i}...")
            run_train()

    @task
    def register_model():
        """Register the model using MLflow REST API via the Hook."""
        hook = MLflowClientHook(mlflow_conn_id=MLFLOW_CONN_ID)

        response = hook.run(
            endpoint="api/2.0/mlflow/registered-models/create",
            request_params={"name": REGISTERED_MODEL_NAME}
        )

        print(f"Registered model response: {json.dumps(response.json(), indent=2)}")

    # Task execution order
    setup_minio_task = setup_minio()
    experiment_task = create_experiment_if_not_exists()
    train_task = train_model_task()
    register_task = register_model()

    setup_minio_task >> experiment_task >> train_task >> register_task

mlflow_training_dag()
