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
