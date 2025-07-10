import os
import pickle
import click
import mlflow
import mlflow.sklearn  
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error


mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("FirstRA_test02")
# Enable MLflow autologging
mlflow.sklearn.autolog()

def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


@click.command()
@click.option(
    "--data_path",
    default="./output",
    help="Location where the processed NYC taxi trip data was saved"
)


def run_train(data_path: str):


    with mlflow.start_run(run_name="baseline_traditionallogging"):  # Start an MLflow run to track this training
        mlflow.set_tag("model_type", "RandomForest02")
        mlflow.set_tag("data_version", "v1.0")
        mlflow.set_tag("mlflow.user", "Ismail")
        X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
        X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))
        # Model parameters
        max_depth = 10
        random_state = 0

        # Manual logging of parameters
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("random_state", random_state)

        rf = RandomForestRegressor(max_depth=max_depth, random_state=random_state)
        print("Fitting model...")
        rf.fit(X_train, y_train)
        mlflow.sklearn.log_model(rf, "model") 
        print("Model fit complete.")

        y_pred = rf.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        mlflow.log_metric("rmse", rmse)
        print(f"RMSE: {rmse:.2f}")
if __name__ == "__main__":
    run_train()