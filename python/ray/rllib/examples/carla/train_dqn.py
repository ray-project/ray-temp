from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_model
from scenarios import TOWN2_ONE_CURVE

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 80,
    "y_res": 80,
    "discrete_actions": True,
    "server_map": "/Game/Maps/Town02",
    "reward_function": "custom",
    "scenarios": TOWN2_ONE_CURVE,
})

register_env(env_name, lambda env_config: CarlaEnv(env_config))
register_carla_model()

ray.init()
run_experiments({
    "carla-dqn": {
        "run": "DQN",
        "env": "carla_env",
        "config": {
            "env_config": env_config,
            "model": {
                "custom_model": "carla",
                "custom_options": {
                    "image_shape": [
                        80,
                        80,
                        lambda spec: spec.config.env_config.framestack * (spec.config.env_config.use_depth_camera and 1 or 3),
                    ],
                },
                "conv_filters": [
                    [16, [8, 8], 4],
                    [32, [4, 4], 2],
                    [512, [10, 10], 1],
                ],
            },
            "timesteps_per_iteration": 100,
            "learning_starts": 1000,
            "schedule_max_timesteps": 100000,
            "gamma": 0.8,
            "tf_session_args": {
                "gpu_options": {
                    "allow_growth": True
                },
            },
        },
    },
})
