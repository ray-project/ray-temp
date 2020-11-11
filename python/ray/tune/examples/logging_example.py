#!/usr/bin/env python

import argparse
import time

from ray import tune


class TestLogger(tune.logger.Logger):
    def on_result(self, result):
        print("TestLogger", result)


def trial_str_creator(trial):
    return "{}_{}_123".format(trial.trainable_name, trial.trial_id)


def evaluation_fn(step, width, height):
    time.sleep(0.1)
    return (0.1 + width * step / 100)**(-1) + height * 0.1


def easy_objective(config):
    # Hyperparameters
    width, height = config["width"], config["height"]

    for step in range(config["steps"]):
        # Iterative training function - can be any arbitrary training procedure
        intermediate_score = evaluation_fn(step, width, height)
        # Feed the score back back to Tune.
        tune.report(iterations=step, mean_loss=intermediate_score)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    analysis = tune.run(
        easy_objective,
        name="hyperband_test",
        num_samples=5,
        trial_name_creator=trial_str_creator,
        loggers=[TestLogger],
        stop={"training_iteration": 1 if args.smoke_test else 99999},
        config={
            "width": tune.randint(10, 100),
            "height": tune.loguniform(10, 100)
        })
    print("Best hyperparameters: ", analysis.best_config)
