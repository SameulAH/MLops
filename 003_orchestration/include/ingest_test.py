# import os
# import logging
# import pandas as pd
# import boto3
# from io import BytesIO
# from datetime import datetime

# # ----------------------------
# # Setup Logging
# # ----------------------------
# os.makedirs("logs", exist_ok=True)
# logging.basicConfig(
#     filename="logs/ingestion_minio.log",
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # ----------------------------
# # Config
# # ----------------------------
# DRY_RUN = False  # Set to False to actually upload
# MINIO_ENDPOINT = "localhost:9001"
# MINIO_BUCKET = "mlops"
# MINIO_ACCESS_KEY = "minioadmin"
# MINIO_SECRET_KEY = "minioadmin"

# YELLOW_TRIPDATA = {
#     "yellow_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
#     "yellow_tripdata_2023-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
#     "yellow_tripdata_2023-03.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
# }

# # ----------------------------
# # Upload Function (with folder)
# # ----------------------------
# def upload_to_minio(url, object_name, folder="raw"):
#     os.makedirs("logs", exist_ok=True)

#     logging.basicConfig(
#         filename=f"logs/minio_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s"
#     )
#     try:
#         logging.info(f"Downloading from: {url}")
#         df = pd.read_parquet(url)
#         logging.info(f"Rows: {len(df)} | Columns: {list(df.columns)}")

#         if DRY_RUN:
#             logging.info("Dry run — skipping upload")
#             return

#         buffer = BytesIO()
#         df.to_parquet(buffer, index=False)
#         buffer.seek(0)

#         s3 = boto3.client(
#             "s3",
#             endpoint_url=f"http://{MINIO_ENDPOINT}",
#             aws_access_key_id=MINIO_ACCESS_KEY,
#             aws_secret_access_key=MINIO_SECRET_KEY,
#         )

#         # Ensure bucket exists
#         try:
#             s3.head_bucket(Bucket=MINIO_BUCKET)
#         except Exception:
#             s3.create_bucket(Bucket=MINIO_BUCKET)
#             logging.info(f"Bucket '{MINIO_BUCKET}' created")

#         full_path = f"{folder}/{object_name}"
#         s3.upload_fileobj(buffer, MINIO_BUCKET, full_path)
#         logging.info(f"Uploaded to s3://{MINIO_BUCKET}/{full_path}")

#     except Exception as e:
#         logging.error(f"Failed to process {url} — {e}")

# # ----------------------------
# # Main Execution
# # ----------------------------
# if __name__ == "__main__":
#     for name, url in YELLOW_TRIPDATA.items():
#         upload_to_minio(url, name, folder="raw")

#     logging.info("All files processed")
# ingest.py

import pandas as pd
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def process_data(url: str) -> pd.DataFrame:
    """Download and preprocess data from a given Parquet URL."""
    df = pd.read_parquet(url)
    df.drop_duplicates(inplace=True)
    return df

def upload_to_minio(df: pd.DataFrame, filename: str, folder: str = "raw", aws_conn_id: str = "minio_s3"):
    """Upload DataFrame to MinIO using S3Hook."""
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    bucket_name = "mlops"
    key = f"{folder}/{filename}"

    # Ensure bucket exists
    if not s3_hook.check_for_bucket(bucket_name):
        client = s3_hook.get_conn()
        client.create_bucket(Bucket=bucket_name)

    # Upload DataFrame
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3_hook.load_file_obj(
        file_obj=buffer,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"Uploaded to MinIO: s3://{bucket_name}/{key}")
