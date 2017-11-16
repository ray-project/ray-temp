# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.ppo.filter import NoFilter, MeanStdFilter


def rollout(policy, env, timestep_limit=None, add_noise=False):
    """Do a rollout.

    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.
    """
    env_timestep_limit = env.spec.tags.get("wrapper_config.TimeLimit"
                                           ".max_episode_steps")
    timestep_limit = (env_timestep_limit if timestep_limit is None
                      else min(timestep_limit, env_timestep_limit))
    rews = []
    t = 0
    observation = env.reset()
    for _ in range(timestep_limit):
        ac = policy.compute(observation[None], add_noise=add_noise)[0]
        observation, rew, done, _ = env.step(ac)
        rews.append(rew)
        t += 1
        if done:
            break
    rews = np.array(rews, dtype=np.float32)
    return rews, t


class GenericPolicy(object):
    def __init__(self, sess, action_space, preprocessor,
                 observation_filter, action_noise_std):
        self.sess = sess
        self.action_space = action_space
        self.action_noise_std = action_noise_std
        self.preprocessor = preprocessor

        if observation_filter == "MeanStdFilter":
            self.observation_filter = MeanStdFilter(
                self.preprocessor.shape, clip=None)
        elif observation_filter == "NoFilter":
            self.observation_filter = NoFilter()
        else:
            raise Exception("Unknown observation_filter: " +
                            str("observation_filter"))

        self.inputs = tf.placeholder(
            tf.float32, [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, dist_type="deterministic")
        model = ModelCatalog.get_model(self.inputs, dist_dim)
        dist = dist_class(model.outputs)
        self.sampler = dist.sample()

        self.variables = ray.experimental.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum([np.prod(variable.shape.as_list())
                               for _, variable
                               in self.variables.variables.items()])
        self.sess.run(tf.global_variables_initializer())

    def compute(self, observation, add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation, update=update)
        action = self.sess.run(self.sampler,
                               feed_dict={self.inputs: observation})
        if add_noise:
            action += np.random.randn(*action.shape) * self.action_noise_std
        return action

    def set_weights(self, x):
        self.variables.set_flat(x)

    def get_weights(self):
        return self.variables.get_flat()
