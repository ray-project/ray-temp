from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_model
from scenarios import TOWN2_STRAIGHT

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 240,
    "y_res": 240,
    "use_depth_camera": False,
    "discrete_actions": True,
    "server_map": "/Game/Maps/Town02",
    "scenarios": TOWN2_STRAIGHT,
})

register_env(env_name, lambda: CarlaEnv(env_config))
register_carla_model()

run_experiments({
    "carla": {
        "run": "DQN",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "model": {
                "custom_model": "carla",
            },
            "timesteps_per_iteration": 100,
            "learning_starts": 1000,
            "schedule_max_timesteps": 100000,
            "gamma": 0.95,
            "tf_session_args": {
              "gpu_options": {"allow_growth": True},
            },
        },
    },
})
