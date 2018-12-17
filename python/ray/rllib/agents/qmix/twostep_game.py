from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete

import ray
from ray.tune import run_experiments, function
from ray.rllib.env.constants import AVAIL_ACTIONS_KEY
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.agents.dqn.dqn_policy_graph import DQNPolicyGraph


class TwoStepGame(MultiAgentEnv):
    """The two-step game from https://arxiv.org/pdf/1803.11485.pdf"""

    def __init__(self, env_config):
        self.state = None
        self.action_space = Discrete(2)
        # Each agent gets a separate obs space, to ensure that they can learn
        # meaningfully different Q values even with a shared Q model.
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
                AVAIL_ACTIONS_KEY: [1, 1]
            },
            "agent_2": {
                AVAIL_ACTIONS_KEY: [1, 1]
            },
        }
        return obs, rewards, dones, infos


if __name__ == "__main__":
    ray.init()
    # Naive use of independent policies converges to 7 reward instead of 8.
    # Actually it will eventually get to 8 reward but only after a long time.
    run_experiments({
        "two_step": {
            "run": "DQN",
            "env": TwoStepGame,
            "config": {
                "multiagent": {
                    "policy_graphs": {
                        "agent_1": (DQNPolicyGraph, Discrete(6), Discrete(2),
                                    {}),
                        "agent_2": (DQNPolicyGraph, Discrete(6), Discrete(2),
                                    {}),
                    },
                    "policy_mapping_fn": function(lambda agent_id: agent_id),
                },
            },
        }
    })
