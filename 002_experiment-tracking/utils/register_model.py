import os
import pickle
import click
import mlflow
import numpy as np
from mlflow.entities import ViewType
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

HPO_EXPERIMENT_NAME = "random-forest-hyperopt"
EXPERIMENT_NAME = "bestRF_model"
RF_PARAMS = ['max_depth', 'n_estimators', 'min_samples_split', 'min_samples_leaf', 'random_state']

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("bestRF_model")
mlflow.sklearn.autolog()


def load_pickle(filename):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


def train_and_log_model(data_path, params):
    X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
    X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))
    X_test, y_test = load_pickle(os.path.join(data_path, "test.pkl"))

    with mlflow.start_run():
        new_params = {}
        for param in RF_PARAMS:
            new_params[param] = int(params[param])

        rf = RandomForestRegressor(**new_params)
        rf.fit(X_train, y_train)

        # Evaluate model on the validation and test sets
        val_rmse = np.sqrt(mean_squared_error(y_val, rf.predict(X_val)))
        #val_rmse = mean_squared_error(y_val, rf.predict(X_val), squared=False)
        mlflow.log_metric("val_rmse", val_rmse)
        #test_rmse = mean_squared_error(y_test, rf.predict(X_test), squared=False)
        test_rmse = np.sqrt(mean_squared_error(y_test, rf.predict(X_test)))
        mlflow.log_metric("test_rmse", test_rmse)


@click.command()
@click.option(
    "--data_path",
    default="./output",
    help="Location where the processed NYC taxi trip data was saved"
)
@click.option(
    "--top_n",
    default=5,
    type=int,
    help="Number of top models that need to be evaluated to decide which one to promote"
)


def run_register_model(data_path: str, top_n: int):

    client = MlflowClient()

    # Step 1: Get top N runs from hyperopt experiment
    hpo_experiment = client.get_experiment_by_name(HPO_EXPERIMENT_NAME)
    hpo_runs = client.search_runs(
        experiment_ids=hpo_experiment.experiment_id,
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=top_n,
        order_by=["metrics.rmse ASC"]
    )

    # Step 2: Retrain and log models to new experiment
    run_ids = []
    for run in hpo_runs:
        run_id = train_and_log_model(data_path=data_path, params=run.data.params)
        run_ids.append(run_id)

    # Step 3: Select best model based on test_rmse
    best_exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    best_run = client.search_runs(
        experiment_ids=best_exp.experiment_id,
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=1,
        order_by=["metrics.test_rmse ASC"]
    )[0]

    # Step 4: Register the best model
    best_model_uri = f"runs:/{best_run.info.run_id}/model"
    mlflow.register_model(model_uri=best_model_uri, name="bestRFModel")
    print(f"Registered model: {best_model_uri}")

    print(f"Registered model from run_id: {best_run.info.run_id}")
    print(f"Test RMSE of the best model: {best_run.data.metrics['test_rmse']}")


if __name__ == '__main__':
    run_register_model()