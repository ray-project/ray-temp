#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import collections
import json
import os
import pickle
import shelve
from pathlib import Path

import gym
import ray
from ray.rllib.agents.registry import get_agent_class
from ray.rllib.env import MultiAgentEnv
from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.evaluation.episode import _flatten_action
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.tune.result import EXPR_PARAM_PICKLE_FILE
from ray.tune.trial import TrialDirSchema
from ray.tune.util import merge_dicts

EXAMPLE_USAGE = """
Example Usage via RLlib CLI:
    rllib rollout /tmp/ray/checkpoint_dir/checkpoint_0/checkpoint-0 --run DQN
    --env CartPole-v0 --steps 1000000 --out rollouts.pkl

Example Usage via executable:
    ./rollout.py /tmp/ray/checkpoint_dir/checkpoint_0/checkpoint-0 --run DQN
    --env CartPole-v0 --steps 1000000 --out rollouts.pkl
"""

# Note: if you use any custom models or envs, register them here first, e.g.:
#
# ModelCatalog.register_custom_model("pa_model", ParametricActionsModel)
# register_env("pa_cartpole", lambda _: ParametricActionCartpole(10))


class RolloutSaver:
    """Utility class for storing rollouts.

    Currently supports two behaviours: the original, which
    simply dumps everything to a pickle file once complete,
    and a mode which stores each rollout as an entry in a Python
    shelf db file. The latter mode is more robust to memory problems
    or crashes part-way through the rollout generation. Each rollout
    is stored with a key based on the episode number (0-indexed),
    and the number of episodes is stored with the key "num_episodes",
    so to load the shelf file, use something like:

    with shelve.open('rollouts.pkl') as rollouts:
       for episode_index in range(rollouts["num_episodes"]):
          rollout = rollouts[str(episode_index)]

    If outfile is None, this class does nothing.
    """

    def __init__(self,
                 outfile=None,
                 use_shelve=False,
                 write_update_file=False,
                 target_steps=None,
                 target_episodes=None,
                 save_info=False):
        self._outfile = outfile
        self._update_file = None
        self._use_shelve = use_shelve
        self._write_update_file = write_update_file
        self._shelf = None
        self._num_episodes = 0
        self._rollouts = []
        self._current_rollout = []
        self._total_steps = 0
        self._target_episodes = target_episodes
        self._target_steps = target_steps
        self._save_info = save_info

    def _get_tmp_progress_filename(self):
        outpath = Path(self._outfile)
        return outpath.parent / ("__progress_" + outpath.name)

    @property
    def outfile(self):
        return self._outfile

    def __enter__(self):
        if self._outfile:
            if self._use_shelve:
                # Open a shelf file to store each rollout as they come in
                self._shelf = shelve.open(self._outfile)
            else:
                # Original behaviour - keep all rollouts in memory and save
                # them all at the end.
                # But check we can actually write to the outfile before going
                # through the effort of generating the rollouts:
                try:
                    with open(self._outfile, "wb") as _:
                        pass
                except IOError as x:
                    print("Can not open {} for writing - cancelling rollouts.".
                          format(self._outfile))
                    raise x
            if self._write_update_file:
                # Open a file to track rollout progress:
                self._update_file = self._get_tmp_progress_filename().open(
                    mode="w")
        return self

    def __exit__(self, type, value, traceback):
        if self._shelf:
            # Close the shelf file, and store the number of episodes for ease
            self._shelf["num_episodes"] = self._num_episodes
            self._shelf.close()
        elif self._outfile and not self._use_shelve:
            # Dump everything as one big pickle:
            pickle.dump(self._rollouts, open(self._outfile, "wb"))
        if self._update_file:
            # Remove the temp progress file:
            self._get_tmp_progress_filename().unlink()
            self._update_file = None

    def _get_progress(self):
        if self._target_episodes:
            return "{} / {} episodes completed".format(self._num_episodes,
                                                       self._target_episodes)
        elif self._target_steps:
            return "{} / {} steps completed".format(self._total_steps,
                                                    self._target_steps)
        else:
            return "{} episodes completed".format(self._num_episodes)

    def begin_rollout(self):
        self._current_rollout = []

    def end_rollout(self):
        if self._outfile:
            if self._use_shelve:
                # Save this episode as a new entry in the shelf database,
                # using the episode number as the key.
                self._shelf[str(self._num_episodes)] = self._current_rollout
            else:
                # Append this rollout to our list, to save laer.
                self._rollouts.append(self._current_rollout)
        self._num_episodes += 1
        if self._update_file:
            self._update_file.seek(0)
            self._update_file.write(self._get_progress() + "\n")
            self._update_file.flush()

    def append_step(self, obs, action, next_obs, reward, done, info):
        """Add a step to the current rollout, if we are saving them"""
        if self._outfile:
            if self._save_info:
                self._current_rollout.append(
                    [obs, action, next_obs, reward, done, info])
            else:
                self._current_rollout.append(
                    [obs, action, next_obs, reward, done])
        self._total_steps += 1


def create_parser(parser_creator=None):
    parser_creator = parser_creator or argparse.ArgumentParser
    parser = parser_creator(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Roll out a reinforcement learning agent "
        "given a checkpoint.",
        epilog=EXAMPLE_USAGE)

    parser.add_argument(
        "checkpoint", type=str, help="Checkpoint from which to roll out.")
    required_named = parser.add_argument_group("required named arguments")
    required_named.add_argument(
        "--run",
        type=str,
        required=True,
        help="The algorithm or model to train. This may refer to the name "
        "of a built-on algorithm (e.g. RLLib's DQN or PPO), or a "
        "user-defined trainable function or class registered in the "
        "tune registry.")
    required_named.add_argument(
        "--env", type=str, help="The gym environment to use.")
    parser.add_argument(
        "--no-render",
        default=False,
        action="store_const",
        const=True,
        help="Surpress rendering of the environment.")
    parser.add_argument(
        "--monitor",
        default=False,
        action="store_const",
        const=True,
        help="Wrap environment in gym Monitor to record video.")
    parser.add_argument(
        "--steps", default=10000, help="Number of steps to roll out.")
    parser.add_argument("--out", default=None, help="Output filename.")
    parser.add_argument(
        "--config",
        default="{}",
        type=json.loads,
        help="Algorithm-specific configuration (e.g. env, hyperparams). "
        "Surpresses loading of configuration from checkpoint.")
    parser.add_argument(
        "--episodes",
        default=0,
        help="Number of complete episodes to roll out. (Overrides --steps)")
    parser.add_argument(
        "--save-info",
        default=False,
        action="store_true",
        help="Save the info field generated by the step() method, "
        "as well as the action, observations, rewards and done fields.")
    parser.add_argument(
        "--use-shelve",
        default=False,
        action="store_true",
        help="Save rollouts into a python shelf file (will save each episode "
        "as it is generated). An output filename must be set using --out.")
    parser.add_argument(
        "--track-progress",
        default=False,
        action="store_true",
        help="Write progress to a temporary file (updated "
        "after each episode). An output filename must be set using --out; "
        "the progress file will live in the same folder.")
    return parser


def run(args, parser):
    # Find configuration file.
    trial_dir = TrialDirSchema.root_from(args.checkpoint)
    checkpoint_n_dir = os.path.dirname(args.checkpoint)
    checkpoint_dir = os.path.dirname(checkpoint_n_dir)
    candidate_dirs = [trial_dir, checkpoint_dir, checkpoint_n_dir]
    config_path = None
    for dir_path in candidate_dirs:
        config_path = os.path.join(dir_path, EXPR_PARAM_PICKLE_FILE)
        if os.path.exists(config_path):
            break
    # Load configuration from file.
    config = {}
    if not config_path:
        if not args.config:
            raise ValueError("Could not find {} in {}.".format(
                EXPR_PARAM_PICKLE_FILE, candidate_dirs))
    else:
        with open(config_path, "rb") as f:
            config = pickle.load(f)
        if "num_workers" in config:
            config["num_workers"] = min(2, config["num_workers"])
    config = merge_dicts(config, args.config)
    if not args.env:
        if not config.get("env"):
            parser.error("the following arguments are required: --env")
        args.env = config.get("env")

    ray.init()

    cls = get_agent_class(args.run)
    agent = cls(env=args.env, config=config)
    agent.restore(args.checkpoint)
    num_steps = int(args.steps)
    num_episodes = int(args.episodes)
    with RolloutSaver(
            args.out,
            args.use_shelve,
            write_update_file=args.track_progress,
            target_steps=num_steps,
            target_episodes=num_episodes,
            save_info=args.save_info) as saver:
        rollout(agent, args.env, num_steps, num_episodes, saver,
                args.no_render, args.monitor)


class DefaultMapping(collections.defaultdict):
    """default_factory now takes as an argument the missing key."""

    def __missing__(self, key):
        self[key] = value = self.default_factory(key)
        return value


def default_policy_agent_mapping(unused_agent_id):
    return DEFAULT_POLICY_ID


def keep_going(steps, num_steps, episodes, num_episodes):
    """Determine whether we've collected enough data"""
    # if num_episodes is set, this overrides num_steps
    if num_episodes:
        return episodes < num_episodes
    # if num_steps is set, continue until we reach the limit
    if num_steps:
        return steps < num_steps
    # otherwise keep going forever
    return True


def rollout(agent,
            env_name,
            num_steps,
            num_episodes=0,
            saver=RolloutSaver(),
            no_render=True,
            monitor=False):
    policy_agent_mapping = default_policy_agent_mapping

    if hasattr(agent, "workers"):
        env = agent.workers.local_worker().env
        multiagent = isinstance(env, MultiAgentEnv)
        if agent.workers.local_worker().multiagent:
            policy_agent_mapping = agent.config["multiagent"][
                "policy_mapping_fn"]

        policy_map = agent.workers.local_worker().policy_map
        state_init = {p: m.get_initial_state() for p, m in policy_map.items()}
        use_lstm = {p: len(s) > 0 for p, s in state_init.items()}
        action_init = {
            p: _flatten_action(m.action_space.sample())
            for p, m in policy_map.items()
        }
    else:
        env = gym.make(env_name)
        multiagent = False
        use_lstm = {DEFAULT_POLICY_ID: False}

    if monitor and not no_render and saver and saver.outfile is not None:
        # If monitoring has been requested,
        # manually wrap our environment with a gym monitor
        # which is set to record every episode.
        env = gym.wrappers.Monitor(
            env, os.path.join(os.path.dirname(saver.outfile), "monitor"),
            lambda x: True)

    steps = 0
    episodes = 0
    while keep_going(steps, num_steps, episodes, num_episodes):
        mapping_cache = {}  # in case policy_agent_mapping is stochastic
        saver.begin_rollout()
        obs = env.reset()
        agent_states = DefaultMapping(
            lambda agent_id: state_init[mapping_cache[agent_id]])
        prev_actions = DefaultMapping(
            lambda agent_id: action_init[mapping_cache[agent_id]])
        prev_rewards = collections.defaultdict(lambda: 0.)
        done = False
        reward_total = 0.0
        while not done and keep_going(steps, num_steps, episodes,
                                      num_episodes):
            multi_obs = obs if multiagent else {_DUMMY_AGENT_ID: obs}
            action_dict = {}
            for agent_id, a_obs in multi_obs.items():
                if a_obs is not None:
                    policy_id = mapping_cache.setdefault(
                        agent_id, policy_agent_mapping(agent_id))
                    p_use_lstm = use_lstm[policy_id]
                    if p_use_lstm:
                        a_action, p_state, _ = agent.compute_action(
                            a_obs,
                            state=agent_states[agent_id],
                            prev_action=prev_actions[agent_id],
                            prev_reward=prev_rewards[agent_id],
                            policy_id=policy_id)
                        agent_states[agent_id] = p_state
                    else:
                        a_action = agent.compute_action(
                            a_obs,
                            prev_action=prev_actions[agent_id],
                            prev_reward=prev_rewards[agent_id],
                            policy_id=policy_id)
                    a_action = _flatten_action(a_action)  # tuple actions
                    action_dict[agent_id] = a_action
                    prev_actions[agent_id] = a_action
            action = action_dict

            action = action if multiagent else action[_DUMMY_AGENT_ID]
            next_obs, reward, done, info = env.step(action)
            if multiagent:
                for agent_id, r in reward.items():
                    prev_rewards[agent_id] = r
            else:
                prev_rewards[_DUMMY_AGENT_ID] = reward

            if multiagent:
                done = done["__all__"]
                reward_total += sum(reward.values())
            else:
                reward_total += reward
            if not no_render:
                env.render()
            saver.append_step(obs, action, next_obs, reward, done, info)
            steps += 1
            obs = next_obs
        saver.end_rollout()
        print("Episode #{}: reward: {}".format(episodes, reward_total))
        if done:
            episodes += 1


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    run(args, parser)
