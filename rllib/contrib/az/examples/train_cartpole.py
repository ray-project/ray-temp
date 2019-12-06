"""Example of using training on CartPole."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

from ray import tune

from rllib.contrib.az.models.custom_torch_models import DenseModel
from rllib.contrib.az.environments.cartpole import CartPole
from rllib.models.catalog import ModelCatalog

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--num_workers', default=6, type=int)
    args = parser.parse_args()

    ModelCatalog.register_custom_model("dense_model", DenseModel)

    # tune.run("PG", config={"env": CartPole})
    tune.run(
        "contrib/AlphaZero",
        max_failures=0,
        config={
            "env": CartPole,
            "num_workers": args.num_workers,
            "sample_batch_size": 50,
            "train_batch_size": 500,
            "sgd_minibatch_size": 64,
            "lr": 1e-4,
            "num_sgd_iter": 1,

            "mcts_config": {
                "puct_coefficient": 1.5,
                "num_simulations": 100,
                "temperature": 1.0,
                "dirichlet_epsilon": 0.20,
                "dirichlet_noise": 0.03,
                "argmax_tree_policy": False,
                "add_dirichlet_noise": True,
            },

            "ranked_rewards": {
                "enable": True,
            },

            "model": {
                "custom_model": "dense_model",
            },
        },
    )
