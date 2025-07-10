# dags/preprocessing_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from include.preprocessing import run_preprocessing

with DAG(
    dag_id="preprocess_yellow_tripdata_minio",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["yellow-taxi", "preprocessing", "mlops"],
) as dag:

    preprocess_task = PythonOperator(
        task_id="run_preprocessing",
        python_callable=run_preprocessing,
    )
