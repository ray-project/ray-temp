"""A simple multi-agent env with two agents playing rock paper scissors.

This demonstrates running the following policies in competition:
    (1) heuristic policy of repeating the same move
    (2) heuristic policy of beating the last opponent move
    (3) LSTM/feedforward PG policies
    (4) LSTM policy with custom entropy loss
"""

import argparse
import os
import random

from ray import tune
from ray.rllib.agents.pg import PGTrainer, PGTFPolicy, PGTorchPolicy
from ray.rllib.agents.registry import get_trainer_class
from pettingzoo.classic import rps_v1
from ray.rllib.examples.policy.rock_paper_scissors_dummies import \
    BeatLastHeuristic, AlwaysSameHeuristic
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env
from ray.rllib.env import PettingZooEnv

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=150)
parser.add_argument("--stop-reward", type=float, default=1000.0)
parser.add_argument("--stop-timesteps", type=int, default=100000)


def env_creator(args):
    env = rps_v1.env()
    return env


register_env("RockPaperScissors",
             lambda config: PettingZooEnv(env_creator(config)))

env_for_spaces = PettingZooEnv(env_creator({}))
obs_space = env_for_spaces.observation_space
act_space = env_for_spaces.action_space


def run_same_policy(args, stop):
    """Use the same policy for both agents (trivial case)."""
    config = {
        "env": "RockPaperScissors",
        "framework": "torch" if args.torch else "tf",
    }

    results = tune.run("PG", config=config, stop=stop, verbose=1)

    if args.as_test:
        # Check vs 0.0 as we are playing a zero-sum game.
        check_learning_achieved(results, 0.0)


def run_heuristic_vs_learned(args, use_lstm=False, trainer="PG"):
    """Run heuristic policies vs a learned agent.

    The learned agent should eventually reach a reward of ~5 with
    use_lstm=False, and ~7 with use_lstm=True. The reason the LSTM policy
    can perform better is since it can distinguish between the always_same vs
    beat_last heuristics.
    """

    def select_policy(agent_id):
        if agent_id == "player_0":
            return "learned"
        else:
            return random.choice(["always_same", "beat_last"])

    config = {
        "env": "RockPaperScissors",
        "gamma": 0.9,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 0,
        "num_envs_per_worker": 4,
        "rollout_fragment_length": 10,
        "train_batch_size": 200,
        "metrics_smoothing_episodes": 200,
        "multiagent": {
            "policies_to_train": ["learned"],
            "policies": {
                "always_same": (AlwaysSameHeuristic, obs_space, act_space, {}),
                "beat_last": (BeatLastHeuristic, obs_space, act_space, {}),
                "learned": (None, obs_space, act_space, {
                    "model": {
                        "use_lstm": use_lstm
                    },
                    "framework": "torch" if args.torch else "tf",
                }),
            },
            "policy_mapping_fn": select_policy,
        },
        "framework": "torch" if args.torch else "tf",
    }
    cls = get_trainer_class(trainer) if isinstance(trainer, str) else trainer
    trainer_obj = cls(config=config)
    for _ in range(args.stop_iters):
        results = trainer_obj.train()
        # Timesteps reached.
        if "policy_always_same_reward" not in results["hist_stats"]:
            reward_diff = 0
            continue
        reward_diff = sum(results["hist_stats"]["policy_learned_reward"])
        if results["timesteps_total"] > args.stop_timesteps:
            break
        # Reward (difference) reached -> all good, return.
        elif reward_diff > args.stop_reward:
            return

    # Reward (difference) not reached: Error if `as_test`.
    if args.as_test:
        raise ValueError(
            "Desired reward difference ({}) not reached! Only got to {}.".
            format(args.stop_reward, reward_diff))


def run_with_custom_entropy_loss(args, stop):
    """Example of customizing the loss function of an existing policy.

    This performs about the same as the default loss does."""

    def entropy_policy_gradient_loss(policy, model, dist_class, train_batch):
        logits, _ = model.from_batch(train_batch)
        action_dist = dist_class(logits, model)
        if args.torch:
            # required by PGTorchPolicy's stats fn.
            policy.pi_err = torch.tensor([0.0])
            return torch.mean(-0.1 * action_dist.entropy() -
                              (action_dist.logp(train_batch["actions"]) *
                               train_batch["advantages"]))
        else:
            return (-0.1 * action_dist.entropy() - tf.reduce_mean(
                action_dist.logp(train_batch["actions"]) *
                train_batch["advantages"]))

    policy_cls = PGTorchPolicy if args.torch else PGTFPolicy
    EntropyPolicy = policy_cls.with_updates(
        loss_fn=entropy_policy_gradient_loss)

    EntropyLossPG = PGTrainer.with_updates(
        name="EntropyPG", get_policy_class=lambda _: EntropyPolicy)

    run_heuristic_vs_learned(args, use_lstm=True, trainer=EntropyLossPG)


def main():
    args = parser.parse_args()

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    run_same_policy(args, stop=stop)
    print("run_same_policy: ok.")

    run_heuristic_vs_learned(args, use_lstm=False)
    print("run_heuristic_vs_learned(w/o lstm): ok.")

    run_heuristic_vs_learned(args, use_lstm=True)
    print("run_heuristic_vs_learned (w/ lstm): ok.")

    run_with_custom_entropy_loss(args, stop=stop)
    print("run_with_custom_entropy_loss: ok.")


if __name__ == "__main__":
    main()
