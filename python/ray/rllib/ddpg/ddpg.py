# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers import LocalSyncReplayOptimizer, LocalSyncOptimizer
from ray.rllib.agent import Agent
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator, RemoteDDPGEvaluator
from ray.tune.result import TrainingResult
import numpy as np

DEFAULT_CONFIG = {
    "actor_model": {"fcnet_activation": "tanh"},
    "critic_model": {"fcnet_activation": "tanh"},
    "env_config": {},
    "gamma": 0.99,
    "horizon": 500,
    "actor_lr": 0.0001,
    "critic_lr": 0.001,
    "num_local_steps": 1,
    "num_workers": 0,
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        "buffer_size": 10,
        "learning_starts": 10,
        "clip_rewards": False,
        "prioritized_replay": False,
        "train_batch_size": 32,
    },
    "tau": 0.001,
}

class DDPGAgent(Agent):
    _agent_name = "DDPG"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = DDPGEvaluator(
            self.registry, self.env_creator, self.config)
        self.remote_evaluators = [
            RemoteDDPGEvaluator.remote(
                self.registry, self.env_creator, self.config)
            for _ in range(self.config["num_workers"])]
        self.optimizer = LocalSyncReplayOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        # update target
        self.local_evaluator.update_target()
        # generate training result

        episode_rewards = []
        episode_lengths = []
        metric_lists = [a.get_completed_rollout_metrics.remote()
                        for a in self.remote_evaluators]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        avg_reward = np.mean(episode_rewards)
        avg_length = np.mean(episode_lengths)
        timesteps = np.sum(episode_lengths)

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

if __name__ == '__main__':
    import ray
    ray.init()
    agent = DDPGAgent(env="Pendulum-v0")
    for i in range(30):
        r = agent.train()
    r = agent.train()

    from ray.tune.logger import pretty_print
    pretty_print(r)
