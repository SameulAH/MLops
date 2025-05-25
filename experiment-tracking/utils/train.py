import os
import pickle
import click
import mlflow
import mlflow.sklearn  # optional but explicit
from sklearn.metrics import mean_squared_error
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error


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

    # Enable MLflow autologging
    mlflow.sklearn.autolog()

    with mlflow.start_run():  # Start an MLflow run to track this training
        X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
        X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))

        rf = RandomForestRegressor(max_depth=10, random_state=0)
        rf.fit(X_train, y_train)

        y_pred = rf.predict(X_val)
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))

        # You can optionally log RMSE manually too
        mlflow.log_metric("rmse", rmse)
        print(f"RMSE: {rmse:.2f}")
if __name__ == "__main__":
    run_train()