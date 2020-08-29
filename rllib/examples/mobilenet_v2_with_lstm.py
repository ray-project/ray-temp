# Explains/tests Issues:
# https://github.com/ray-project/ray/issues/6928
# https://github.com/ray-project/ray/issues/6732

import argparse
from gym.spaces import Discrete, Box
import numpy as np

from ray import tune
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.examples.models.mobilenet_v2_with_lstm_models import \
    MobileV2PlusRNNModel, TorchMobileV2PlusRNNModel
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import RLLIB_FORCE_NUM_GPUS

tf1, tf, tfv = try_import_tf()

cnn_shape = (4, 4, 3)
# The torch version of MobileNetV2 does channels first.
cnn_shape_torch = (3, 224, 224)

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-reward", type=float, default=0.0)
parser.add_argument("--stop-timesteps", type=int, default=100000)

if __name__ == "__main__":
    args = parser.parse_args()

    # Register our custom model.
    ModelCatalog.register_custom_model(
        "my_model", TorchMobileV2PlusRNNModel
        if args.torch else MobileV2PlusRNNModel)

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Configure our Trainer.
    config = {
        "env": RandomEnv,
        "framework": "torch" if args.torch else "tf",
        "model": {
            "custom_model": "my_model",
            # Extra config passed to the custom model's c'tor as kwargs.
            "custom_model_config": {
                "cnn_shape": cnn_shape_torch if args.torch else cnn_shape,
            },
            "max_seq_len": 20,
        },
        "vf_share_layers": True,
        # Use GPUs iff `RLLIB_FORCE_NUM_GPUS` env var set to > 0.
        "num_gpus": RLLIB_FORCE_NUM_GPUS,
        "num_workers": 0,  # no parallelism
        "env_config": {
            "action_space": Discrete(2),
            # Test a simple Image observation space.
            "observation_space": Box(
                0.0,
                1.0,
                shape=cnn_shape_torch if args.torch else cnn_shape,
                dtype=np.float32)
        },
    }

    tune.run("PPO", config=config, stop=stop, verbose=1)
