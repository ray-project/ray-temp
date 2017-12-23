from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pickle
import os

import ray
from ray.rllib.agent import Agent
from ray.rllib.envs import create_and_wrap
from ray.rllib.optimizers import AsyncOptimizer
from ray.rllib.a3c.base_evaluator import A3CEvaluator, RemoteA3CEvaluator
from ray.rllib.a3c.common import get_policy_cls
from ray.rllib.utils.filter import get_filter
from ray.tune.result import TrainingResult


DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 4,
    # Number of gradients applied for each `train` step
    "grads_per_step": 100,
    # Size of rollout batch
    "batch_size": 10,
    # Use LSTM model - only applicable for image states
    "use_lstm": False,
    # Use PyTorch as backend - no LSTM support
    "use_pytorch": False,
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Which reward filter to apply to the reward
    "reward_filter": "NoFilter",
    # Discount factor of MDP
    "gamma": 0.99,
    # GAE(gamma) parameter
    "lambda": 1.0,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 40.0,
    # Learning rate
    "lr": 0.0001,
    # Preprocessing for environment
    "preprocessing": {
        # (Image statespace) - Converts image to Channels = 1
        "grayscale": True,
        # (Image statespace) - Each pixel
        "zero_mean": False,
        # (Image statespace) - Converts image to (dim, dim, C)
        "dim": 42,
        # (Image statespace) - Converts image shape to (C, dim, dim)
        "channel_major": False},
    # Parameters for Model specification
    "model": {}
}


class A3CAgent(Agent):
    _agent_name = "A3C"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = A3CEvaluator(
            self.env_creator, self.config, self.logdir, start_sampler=False)
        self.remote_evaluators = [
            RemoteA3CEvaluator.remote(
                self.env_creator, self.config, self.logdir)
            for i in range(self.config["num_workers"])]
        self.optimizer = AsyncOptimizer(
            self.config, self.local_evaluator, self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        res = self._fetch_metrics_from_remote_evaluators()
        return res

    def _fetch_metrics_from_remote_evaluators(self):
        episode_rewards = []
        episode_lengths = []
        metric_lists = [a.get_completed_rollout_metrics.remote()
                            for a in self.remote_evaluators]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        avg_reward = (
            np.mean(episode_rewards) if episode_rewards else float('nan'))
        avg_length = (
            np.mean(episode_lengths) if episode_lengths else float('nan'))
        timesteps = np.sum(episode_lengths) if episode_lengths else 0

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

    def _save(self):
        # TODO(rliaw): extend to also support saving worker state?
        checkpoint_path = os.path.join(
            self.logdir, "checkpoint-{}".format(self.iteration))
        objects = [self.parameters, self.obs_filter, self.rew_filter]
        pickle.dump(objects, open(checkpoint_path, "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        objects = pickle.load(open(checkpoint_path, "rb"))
        self.parameters = objects[0]
        self.obs_filter = objects[1]
        self.rew_filter = objects[2]
        self.policy.set_weights(self.parameters)

    def compute_action(self, observation):
        obs = self.obs_filter(observation, update=False)
        action, info = self.policy.compute(obs)
        return action
