#!/usr/bin/env python
"""Examples using MLFlowLoggerCallback and mlflow_mixin.
"""
import os
import tempfile
import time

import mlflow

from ray import tune
from ray.tune.integration.mlflow import MLFlowLoggerCallback, mlflow_mixin


def evaluation_fn(step, width, height):
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config.get("steps", 100)):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


def tune_function(mlflow_tracking_uri):
    tune.run(
        easy_objective,
        name="mlflow",
        num_samples=5,
        callbacks=[
            MLFlowLoggerCallback(
                tracking_uri=mlflow_tracking_uri,
                experiment_name="test",
                save_artifact=True)
        ],
        config={
            "width": tune.randint(10, 100),
            "height": tune.randint(0, 100),
        })


@mlflow_mixin
def decorated_easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config.get("steps", 100)):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        mlflow.log_metrics(dict(mean_loss=intermediate_score), step=step)
        tune.report(iterations=step, mean_loss=intermediate_score)
        time.sleep(0.1)


def tune_decorated(mlflow_tracking_uri):
    # Set the experiment, or create a new one if does not exist yet.
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name="mixin_test")
    tune.run(
        decorated_easy_objective,
        name="mlflow",
        num_samples=5,
        config={
            "width": tune.randint(10, 100),
            "height": tune.randint(0, 100),
            "mlflow": {
                "experiment_name": "mixin_test",
                "tracking_uri": mlflow.get_tracking_uri()
            }
        })


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        mlflow_tracking_uri = os.path.join(tempfile.gettempdir(), "mlruns")
    else:
        mlflow_tracking_uri = None

    tune_function(mlflow_tracking_uri)
    if not args.smoke_test:
        df = mlflow.search_runs(
            [mlflow.get_experiment_by_name("test").experiment_id])
        print(df)

    tune_decorated(mlflow_tracking_uri)
    if not args.smoke_test:
        df = mlflow.search_runs(
            [mlflow.get_experiment_by_name("mixin_test").experiment_id])
        print(df)
