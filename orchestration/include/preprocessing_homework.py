# include/preprocessing.py

import os
import pandas as pd
from io import BytesIO
from typing import Tuple
from sklearn.feature_extraction import DictVectorizer
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.utils import dump_pickle  # Make sure you create this utility

import pickle

def read_from_minio(filename: str, folder: str = "raw", aws_conn_id: str = "minio_s3") -> pd.DataFrame:
    bucket_name = "mlopsdir"
    key = f"{folder}/{filename}"

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    obj = s3_hook.get_key(bucket_name=bucket_name, key=key)
    buffer = BytesIO(obj.get()["Body"].read())
    df = pd.read_parquet(buffer)
    print(f"Read {len(df)} rows from {filename} in MinIO bucket '{bucket_name}'.")
     # Convert datetime columns if necessary
    print(df.columns)
    print(df.shape)
    return df


def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df['duration'] = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)
    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    return df


def featurize(df: pd.DataFrame, dv: DictVectorizer = None, fit: bool = False) -> Tuple:
    categorical = ['PULocationID', 'DOLocationID']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    print(f"DataFrame shape after processing: {df.shape}")

    if fit:
        dv = DictVectorizer()
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)

    return X, dv


def save_pickle_to_minio(obj, filename: str, folder: str = "processed", aws_conn_id: str = "minio_s3"):
    bucket_name = "mlopsdir"
    key = f"{folder}/{filename}"

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    buffer = BytesIO()
    pickle.dump(obj, buffer)
    buffer.seek(0)

    s3_hook.load_file_obj(file_obj=buffer, key=key, bucket_name=bucket_name, replace=True)


def run_preprocessing(aws_conn_id: str = "minio_s3", dataset: str = "yellow"):
    filenames = [
        f"{dataset}_tripdata_2023-01.parquet",
        f"{dataset}_tripdata_2023-02.parquet",
        f"{dataset}_tripdata_2023-03.parquet"
    ]

    df_train = preprocess_dataframe(read_from_minio(filenames[0], aws_conn_id=aws_conn_id))
    df_val = preprocess_dataframe(read_from_minio(filenames[1], aws_conn_id=aws_conn_id))
    df_test = preprocess_dataframe(read_from_minio(filenames[2], aws_conn_id=aws_conn_id))

    y_train, y_val, y_test = df_train['duration'].values, df_val['duration'].values, df_test['duration'].values

    X_train, dv = featurize(df_train, fit=True)
    X_val, _ = featurize(df_val, dv)
    X_test, _ = featurize(df_test, dv)

    save_pickle_to_minio(dv, "dv.pkl", aws_conn_id=aws_conn_id)
    save_pickle_to_minio((X_train, y_train), "train.pkl", aws_conn_id=aws_conn_id)
    save_pickle_to_minio((X_val, y_val), "val.pkl", aws_conn_id=aws_conn_id)
    save_pickle_to_minio((X_test, y_test), "test.pkl", aws_conn_id=aws_conn_id)
