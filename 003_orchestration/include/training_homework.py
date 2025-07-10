import os
import pickle
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



# Set up the MLflow tracking URI and experiment
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("lr_experiment-HW")

# Enable MLflow autologging
mlflow.sklearn.autolog(disable=True)


def load_pickle_from_minio(filename: str, aws_conn_id: str = "minio_s3") -> pd.DataFrame:
    """Load a pickled DataFrame from MinIO"""
    bucket_name = "mlopsdirhowework"
    key = f"processed/{filename}"

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    obj = s3_hook.get_key(bucket_name=bucket_name, key=key)
    buffer = BytesIO(obj.get()["Body"].read())
    return pickle.load(buffer)


def run_train():
    """Training function that loads data, trains the model, and logs to MLflow"""
    
    with mlflow.start_run(run_name="baseline_traditionallogging"):
        # Set tags for the run
        mlflow.set_tag("model_type", "Linearegression-HW")
        mlflow.set_tag("data_version", "v1.0")
        mlflow.set_tag("mlflow.user", "Ismail")
        
        # Load training and validation data from MinIO
        X_train, y_train = load_pickle_from_minio("train.pkl")
        # X_val, y_val = load_pickle_from_minio("val.pkl")
        

        # Initialize and train the model
        lr = LinearRegression()
        print("Training model...")
        lr.fit(X_train, y_train)
        mlflow.log_param("intercept_", lr.intercept_)

        # Log the model to MLflow
        mlflow.sklearn.log_model(lr, "modelRF-HW")
        print("Model training complete.")
        
        # Predict and evaluate the model
        # y_pred = rf.predict(X_val)
        # rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        
        # # Log evaluation metrics
        # mlflow.log_metric("rmse", rmse)
        # print(f"RMSE: {rmse:.2f}")

if __name__ == "__main__":
    run_train()






