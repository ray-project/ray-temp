"""Example of using two different training methods at once in multi-agent.

Here we create a number of CartPole agents, some of which are trained with
DQN, and some of which are trained with PPO. We periodically sync weights
between the two trainers (note that no such syncing is needed when using just
a single training method).

For a simpler example, see also: multiagent_cartpole.py
"""

import argparse
import gym

import ray
from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.agents.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.agents.ppo.ppo import PPOTrainer
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=20)
parser.add_argument("--stop-reward", type=float, default=50)


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env("multi_agent_cartpole",
                 lambda _: MultiAgentCartPole({"num_agents": 4}))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    # You can also have multiple policies per trainer, but here we just
    # show one each for PPO and DQN.
    policies = {
        "ppo_policy": (PPOTFPolicy, obs_space, act_space, {}),
        "dqn_policy": (DQNTFPolicy, obs_space, act_space, {}),
    }

    def policy_mapping_fn(agent_id):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "dqn_policy"

    ppo_trainer = PPOTrainer(
        env="multi_agent_cartpole",
        config={
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["ppo_policy"],
            },
            "explore": False,
            # disable filters, otherwise we would need to synchronize those
            # as well to the DQN agent
            "observation_filter": "NoFilter",
            "use_pytorch": args.torch,
        })

    dqn_trainer = DQNTrainer(
        env="multi_agent_cartpole",
        config={
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["dqn_policy"],
            },
            "gamma": 0.95,
            "n_step": 3,
            "use_pytorch": args.torch,
        })

    # You should see both the printed X and Y approach 200 as this trains:
    # info:
    #   policy_reward_mean:
    #     dqn_policy: X
    #     ppo_policy: Y
    for i in range(args.stop_iters):
        print("== Iteration", i, "==")

        # improve the DQN policy
        print("-- DQN --")
        TODO: when reaching stop-reward -> stop
        print(pretty_print(dqn_trainer.train()))

        # improve the PPO policy
        print("-- PPO --")
        print(pretty_print(ppo_trainer.train()))

        # swap weights to synchronize
        dqn_trainer.set_weights(ppo_trainer.get_weights(["ppo_policy"]))
        ppo_trainer.set_weights(dqn_trainer.get_weights(["dqn_policy"]))
