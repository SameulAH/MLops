#ingest.py
import os
import pandas as pd
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

# Base local dir to simulate S3 bucket structure (only for validation)
BASE_LOCAL_DIR = os.path.join(os.getcwd(), "mlops_output")


def ensure_local_dirs(bucket: str, folder: str):
    """Ensure the required local directory structure exists (for volume binding/local checks)."""
    full_path = os.path.join(BASE_LOCAL_DIR, bucket, folder)
    if not os.path.exists(full_path):
        os.makedirs(full_path, exist_ok=True)
        logger.info(f"Created local dir: {full_path}")
    else:
        logger.info(f"Local dir already exists: {full_path}")


def process_data(url: str) -> pd.DataFrame:
    """Download and preprocess data from a given Parquet URL."""
    logger.info(f"Reading Parquet data from {url}")
    df = pd.read_parquet(url)
    # df.drop_duplicates(inplace=True)
    return df


def upload_to_minio(df: pd.DataFrame, filename: str, folder: str = "raw", aws_conn_id: str = "minio_s3"):
    """Upload DataFrame to MinIO S3-compatible bucket."""
    bucket_name = "mlopsdir"
    key = f"{folder}/{filename}"

    ensure_local_dirs(bucket_name, folder)

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    client = s3_hook.get_conn()

    if not s3_hook.check_for_bucket(bucket_name):
        logger.info(f"Bucket '{bucket_name}' not found. Creating...")
        client.create_bucket(Bucket=bucket_name)

    # Write to memory and upload
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_hook.load_file_obj(
        file_obj=buffer,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info(f"Uploaded {filename} to s3://{bucket_name}/{key}")
