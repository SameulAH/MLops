import os
import logging
import pandas as pd
import boto3
from io import BytesIO

# ----------------------------
# Setup Logging
# ----------------------------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/ingestion_minio.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------------
# Config
# ----------------------------
DRY_RUN = True  # Set to False to upload to MinIO
parquet_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
minio_endpoint = "localhost:9000"
minio_bucket = "mlops"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
object_path = "raw/yellow_tripdata_2023-01.parquet"

# ----------------------------
# Load and Upload
# ----------------------------
def upload_to_minio(url, object_path):
    try:
        logging.info(f"Downloading dataset from {url}")
        df = pd.read_parquet(url)
        logging.info(f"Read {len(df)} rows. Schema:\n{df.dtypes}")

        if DRY_RUN:
            logging.info("Dry run enabled — skipping MinIO upload")
            return

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{minio_endpoint}",
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
        )

        # Ensure bucket exists
        buckets = s3.list_buckets()
        if not any(b['Name'] == minio_bucket for b in buckets['Buckets']):
            s3.create_bucket(Bucket=minio_bucket)
            logging.info(f"Created bucket '{minio_bucket}'")

        # Upload file
        s3.upload_fileobj(buffer, minio_bucket, object_path)
        logging.info(f"Uploaded dataset to MinIO at: s3://{minio_bucket}/{object_path}")

    except Exception as e:
        logging.error(f"Error during ingestion: {str(e)}")

# ----------------------------
# Run Script
# ----------------------------
if __name__ == "__main__":
    upload_to_minio(parquet_url, object_path)
