# import os
# import pickle
# import mlflow
# import mlflow.sklearn
# import numpy as np
# import pandas as pd
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_squared_error
# from io import BytesIO
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# # Set up the MLflow tracking URI and experiment
# mlflow.set_tracking_uri("http://mlflow:5000")
# mlflow.set_experiment("rf_experiment2")

# # Enable MLflow autologging
# mlflow.sklearn.autolog()

# def load_pickle_from_minio(filename: str, aws_conn_id: str = "minio_s3") -> pd.DataFrame:
#     """Load a pickled DataFrame from MinIO"""
#     bucket_name = "mlopsdir"
#     key = f"processed/{filename}"

#     s3_hook = S3Hook(aws_conn_id=aws_conn_id)
#     obj = s3_hook.get_key(bucket_name=bucket_name, key=key)
#     buffer = BytesIO(obj.get()["Body"].read())
#     return pickle.load(buffer)


# def run_train():
#     """Training function that loads data, trains the model, and logs to MLflow"""
    
#     with mlflow.start_run(run_name="baseline_traditionallogging"):
#         # Set tags for the run
#         mlflow.set_tag("model_type", "RandomForest")
#         mlflow.set_tag("data_version", "v1.0")
#         mlflow.set_tag("mlflow.user", "Ismail")
        
#         # Load training and validation data from MinIO
#         X_train, y_train = load_pickle_from_minio("train.pkl")
#         X_val, y_val = load_pickle_from_minio("val.pkl")
        
#         # Model parameters
#         max_depth = 10
#         random_state = 0
        
#         # Log parameters manually
#         mlflow.log_param("max_depth", max_depth)
#         mlflow.log_param("random_state", random_state)
        
#         # Initialize and train the model
#         rf = RandomForestRegressor(max_depth=max_depth, random_state=random_state)
#         print("Training model...")
#         rf.fit(X_train, y_train)
        
#         # Log the model to MLflow
#         mlflow.sklearn.log_model(rf, "model")
#         print("Model training complete.")
        
#         # Predict and evaluate the model
#         y_pred = rf.predict(X_val)
#         rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        
#         # Log evaluation metrics
#         mlflow.log_metric("rmse", rmse)
#         print(f"RMSE: {rmse:.2f}")

# if __name__ == "__main__":
#     run_train()



import os
import pickle
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import psutil
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import gc  # for manual memory cleanup

# Set MLflow tracking
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("rf_experiment2")

# Only log params and metrics, not full model (save memory)
mlflow.sklearn.autolog(log_models=False)

def load_pickle_from_minio(filename: str, aws_conn_id: str = "minio_s3"):
    bucket_name = "mlopsdir"
    key = f"processed/{filename}"
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    obj = s3_hook.get_key(bucket_name=bucket_name, key=key)
    buffer = BytesIO(obj.get()["Body"].read())
    return pickle.load(buffer)

def print_memory(tag=""):
    used = psutil.virtual_memory().used / 1024 ** 2
    print(f"[MEMORY {tag}] Used: {used:.2f} MB")

def run_train():
    with mlflow.start_run(run_name="baseline_traditionallogging"):
        mlflow.set_tag("model_type", "RandomForest")
        mlflow.set_tag("data_version", "v1.0")
        mlflow.set_tag("mlflow.user", "Ismail")

        print_memory("before data load")
        X_train, y_train = load_pickle_from_minio("train.pkl")
        X_val, y_val = load_pickle_from_minio("val.pkl")
        print_memory("after data load")

        # Sample data to reduce memory load (tweak fraction as needed)
        X_train, y_train = load_pickle_from_minio("train.pkl")
        X_val, y_val = load_pickle_from_minio("val.pkl")
        

        print_memory("after sampling")

        max_depth = 10
        random_state = 0
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("random_state", random_state)

        rf = RandomForestRegressor(max_depth=max_depth, random_state=random_state)
        print("Training model...")
        rf.fit(X_train, y_train)
        print("Model training complete.")

        y_pred = rf.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        mlflow.log_metric("rmse", rmse)
        print(f"RMSE: {rmse:.2f}")

        # Explicit cleanup
        del X_train, y_train, X_val, y_val, rf, y_pred
        gc.collect()
        print_memory("after cleanup")

if __name__ == "__main__":
    run_train()
