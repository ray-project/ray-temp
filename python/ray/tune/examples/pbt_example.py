#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse
import random

import ray
from ray.tune import Trainable, run
from ray.tune.schedulers import PopulationBasedTraining


class PBTBenchmarkExample(Trainable):
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

    def _setup(self, config):
        self.lr = config["lr"]
        self.accuracy = 0.0  # end = 1000

    def _train(self):
        midpoint = 100  # lr starts decreasing after acc > midpoint
        q_tolerance = 3  # penalize exceeding lr by more than this multiple
        noise_level = 2  # add gaussian noise to the acc increase
        # triangle wave:
        #  - start at 0.001 @ t=0,
        #  - peak at 0.01 @ t=midpoint,
        #  - end at 0.001 @ t=midpoint * 2,
        if self.accuracy < midpoint:
            optimal_lr = 0.01 * self.accuracy / midpoint
        else:
            optimal_lr = 0.01 - 0.01 * (self.accuracy - midpoint) / midpoint
        optimal_lr = min(0.01, max(0.001, optimal_lr))

        # compute accuracy increase
        q_err = max(self.lr, optimal_lr) / min(self.lr, optimal_lr)
        if q_err < q_tolerance:
            self.accuracy += (1.0 / q_err) * random.random()
        elif self.lr > optimal_lr:
            self.accuracy -= (q_err - q_tolerance) * random.random()
        self.accuracy += noise_level * np.random.normal()
        self.accuracy = max(0, self.accuracy)

        return {
            "mean_accuracy": self.accuracy,
            "cur_lr": self.lr,
            "optimal_lr": optimal_lr,  # for debugging
            "q_err": q_err,  # for debugging
            "done": self.accuracy > midpoint * 2,
        }

    def _save(self, checkpoint_dir):
        return {
            "accuracy": self.accuracy,
            "lr": self.lr,
        }

    def _restore(self, checkpoint):
        self.accuracy = checkpoint["accuracy"]

    def reset_config(self, new_config):
        self.lr = new_config["lr"]
        return True


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
        reward_attr="mean_accuracy",
        perturbation_interval=20,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: random.uniform(0.0001, 0.02),
            # allow perturbations within this set of categorical values
            "some_other_factor": [1, 2],
        })

    run(PBTBenchmarkExample,
        name="pbt_test",
        scheduler=pbt,
        reuse_actors=True,
        verbose=False,
        **{
            "stop": {
                "training_iteration": 2000,
            },
            "num_samples": 4,
            "config": {
                "lr": 0.0001,
                "some_other_factor": 1,  # note: has no effect
            },
        })
