"""This test checks that Skopt is functional.

It also checks that it is usable with a separate scheduler.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run_experiments, register_trainable
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.suggest import SkoptSearch


def easy_objective(config, reporter):
    import time
    time.sleep(0.2)
    for i in range(config["iterations"]):
        reporter(
            timesteps_total=i,
            neg_mean_loss=-(config["height"] - 14)**2 +
            abs(config["width"] - 3))
        time.sleep(0.02)


if __name__ == '__main__':
    import argparse
    from skopt import Optimizer

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()
    ray.init(redirect_output=True)

    register_trainable("exp", easy_objective)


    config = {
        "skopt": {
            "run": "exp",
            "num_samples": 10 if args.smoke_test else 50,
            "config": {
                "iterations": 100,
            },
            "stop": {
                "timesteps_total": 100
            },
        }
    }
    optimizer = Optimizer([(-2,2),(-2,2)])
    algo = SkoptSearch(
        optimizer, ["height", "width"],
        max_concurrent=4, reward_attr="neg_mean_loss")
    scheduler = AsyncHyperBandScheduler(reward_attr="neg_mean_loss")
    run_experiments(config, search_alg=algo, scheduler=scheduler)
