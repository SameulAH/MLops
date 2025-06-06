from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_pipeline",
    default_args=default_args,
    description="Ingest green taxi data into PostgreSQL",
    schedule_interval=None,  # run manually unless scheduled
    catchup=False,
) as dag:

    ingest_task = BashOperator(
        task_id="run_data_ingestion",
        bash_command="python /opt/airflow/ingest.py"  # Update path if needed
    )

    ingest_task