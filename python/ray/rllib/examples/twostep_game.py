"""The two-step game from QMIX: https://arxiv.org/pdf/1803.11485.pdf"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from gym.spaces import Tuple, Discrete

import ray
from ray.tune import register_env, run_experiments, grid_search
from ray.rllib.env.constants import AVAIL_ACTIONS_KEY
from ray.rllib.env.multi_agent_env import MultiAgentEnv

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=50000)
parser.add_argument("--run", type=str, default="QMIX")


class TwoStepGame(MultiAgentEnv):
    def __init__(self, env_config):
        self.state = None
        self.action_space = Discrete(2)
        # Each agent gets a separate [3] obs space, to ensure that they can
        # learn meaningfully different Q values even with a shared Q model.
        self.observation_space = Discrete(6)

    def reset(self):
        self.state = 0
        return {"agent_1": self.state, "agent_2": self.state + 3}

    def step(self, action_dict):
        if self.state == 0:
            action = action_dict["agent_1"]
            assert action in [0, 1], action
            if action == 0:
                self.state = 1
            else:
                self.state = 2
            global_rew = 0
            done = False
        elif self.state == 1:
            global_rew = 7
            done = True
        else:
            if action_dict["agent_1"] == 0 and action_dict["agent_2"] == 0:
                global_rew = 0
            elif action_dict["agent_1"] == 1 and action_dict["agent_2"] == 1:
                global_rew = 8
            else:
                global_rew = 1
            done = True

        rewards = {"agent_1": global_rew / 2.0, "agent_2": global_rew / 2.0}
        obs = {"agent_1": self.state, "agent_2": self.state + 3}
        dones = {"__all__": done}
        infos = {
            "agent_1": {
                AVAIL_ACTIONS_KEY: [1, 1]  # all actions avail
            },
            "agent_2": {
                AVAIL_ACTIONS_KEY: [1, 1]
            },
        }
        return obs, rewards, dones, infos


if __name__ == "__main__":
    args = parser.parse_args()

    grouping = {
        "group_1": ["agent_1", "agent_2"],
    }
    obs_space = Tuple([Discrete(6), Discrete(6)])
    act_space = Tuple([Discrete(2), Discrete(2)])
    register_env(
        "grouped_twostep",
        lambda config: TwoStepGame(config).with_agent_groups(
            grouping, obs_space=obs_space, act_space=act_space))

    if args.run == "QMIX":
        config = {
            "sample_batch_size": 4,
            "train_batch_size": 32,
            "exploration_final_eps": 0.02,
            "num_workers": 0,
            "mixer": grid_search([None, "qmix", "vdn"]),
        }
    elif args.run == "APEX_QMIX":
        config = {
            "num_gpus": 0,
            "num_workers": 2,
            "optimizer": {
                "num_replay_buffer_shards": 1,
            },
            "min_iter_time_s": 3,
            "buffer_size": 1000,
            "learning_starts": 1000,
            "train_batch_size": 128,
            "sample_batch_size": 32,
            "target_network_update_freq": 500,
            "timesteps_per_iteration": 1000,
        }
    else:
        config = {}

    ray.init()
    run_experiments({
        "two_step": {
            "run": args.run,
            "env": "grouped_twostep",
            "stop": {
                "timesteps_total": args.stop,
            },
            "config": config,
        },
    })
