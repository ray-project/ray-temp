#!/usr/bin/env python

import numpy as np
import argparse
import json
import os
import random

import ray
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining


def pbt_function(config, checkpoint_path=None):
    """Toy PBT problem for benchmarking adaptive learning rate.

    The goal is to optimize this trainable's accuracy. The accuracy increases
    fastest at the optimal lr, which is a function of the current accuracy.

    The optimal lr schedule for this problem is the triangle wave as follows.
    Note that many lr schedules for real models also follow this shape:

     best lr
      ^
      |    /\
      |   /  \
      |  /    \
      | /      \
      ------------> accuracy

    In this problem, using PBT with a population of 2-4 is sufficient to
    roughly approximate this lr schedule. Higher population sizes will yield
    faster convergence. Training will not converge without PBT.
    """
    lr = config["lr"]
    accuracy = 0.0  # end = 1000
    start = 0
    if checkpoint_path:
        with open(checkpoint_path) as f:
            state = json.loads(f.read())
            accuracy = state["acc"]
            start = state["step"]
    midpoint = 100  # lr starts decreasing after acc > midpoint
    q_tolerance = 3  # penalize exceeding lr by more than this multiple
    noise_level = 2  # add gaussian noise to the acc increase
    # triangle wave:
    #  - start at 0.001 @ t=0,
    #  - peak at 0.01 @ t=midpoint,
    #  - end at 0.001 @ t=midpoint * 2,
    for step in range(start, 100):
        if accuracy < midpoint:
            optimal_lr = 0.01 * accuracy / midpoint
        else:
            optimal_lr = 0.01 - 0.01 * (accuracy - midpoint) / midpoint
        optimal_lr = min(0.01, max(0.001, optimal_lr))

        # compute accuracy increase
        q_err = max(lr, optimal_lr) / min(lr, optimal_lr)
        if q_err < q_tolerance:
            accuracy += (1.0 / q_err) * random.random()
        elif lr > optimal_lr:
            accuracy -= (q_err - q_tolerance) * random.random()
        accuracy += noise_level * np.random.normal()
        accuracy = max(0, accuracy)

        if step % 3 == 0:
            checkpoint_dir = tune.make_checkpoint_dir(step=step)
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"acc": accuracy, "step": start}))
            tune.save_checkpoint(path)

        tune.report(
            mean_accuracy=accuracy,
            cur_lr=lr,
            optimal_lr=optimal_lr,  # for debugging
            q_err=q_err,  # for debugging
            done=accuracy > midpoint * 2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    if args.smoke_test:
        ray.init(num_cpus=2)  # force pausing to happen for test
    else:
        ray.init()

    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="mean_accuracy",
        mode="max",
        perturbation_interval=20,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: random.uniform(0.0001, 0.02),
            # allow perturbations within this set of categorical values
            "some_other_factor": [1, 2],
        })

    tune.run(
        pbt_function,
        name="pbt_test",
        scheduler=pbt,
        verbose=False,
        stop={
            "training_iteration": 200,
        },
        num_samples=8,
        fail_fast=True,
        config={
            "lr": 0.0001,
            # note: this parameter is perturbed but has no effect on
            # the model training in this example
            "some_other_factor": 1,
        })
