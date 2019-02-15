"""Example of a custom gym environment. Run this for a demo."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import gym
from gym.spaces import Discrete, Box
from gym.envs.registration import EnvSpec

import ray
from ray.tune import run_experiments


class SimpleCorridor(gym.Env):
    """Example of a custom env in which you have to walk down a corridor.

    You can configure the length of the corridor via the env config."""

    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(
            0.0, self.end_pos, shape=(1, ), dtype=np.float32)
        self._spec = EnvSpec("SimpleCorridor-{}-v0".format(self.end_pos))

    def reset(self):
        self.cur_pos = 0
        return [self.cur_pos]

    def step(self, action):
        assert action in [0, 1], action
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        elif action == 1:
            self.cur_pos += 1
        done = self.cur_pos >= self.end_pos
        return [self.cur_pos], 1 if done else 0, done, {}


if __name__ == "__main__":
    # Can also register the env creator function explicitly with:
    # register_env("corridor", lambda config: SimpleCorridor(config))
    ray.init()
    run_experiments({
        "without-rnd-test": {
            "run": "IMPALA",
            "stop": {
                "episode_reward_mean": 1
            },
            "env": SimpleCorridor,  # or "corridor" if registered above
            "config": {
                "env_config": {
                    "corridor_length": 1000,
                },
                "sample_batch_size": 50,
                "train_batch_size": 500,
                "num_workers": 1,
                "num_envs_per_worker": 10,
                "rnd": 0
            },
        },
    })

