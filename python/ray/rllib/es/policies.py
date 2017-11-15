# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf

import ray
from ray.rllib.models import ModelCatalog


def rollout(policy, env, preprocessor, timestep_limit=None, add_noise=False):
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
    observation = preprocessor.transform(env.reset())
    for _ in range(timestep_limit):
        ac = policy.compute(observation[None], add_noise=add_noise)[0]
        observation, rew, done, _ = env.step(ac)
        observation = preprocessor.transform(observation)
        rews.append(rew)
        t += 1
        if done:
            break
    rews = np.array(rews, dtype=np.float32)
    return rews, t


class GenericPolicy(object):
    def __init__(self, sess, ob_space, ac_space, preprocessor, ac_noise_std):
        self.sess = sess
        self.ac_space = ac_space
        self.ac_noise_std = ac_noise_std
        self.preprocessor = preprocessor

        self.inputs = tf.placeholder(
            tf.float32, [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.ac_space, dist_type='deterministic')
        model = ModelCatalog.get_model(self.inputs, dist_dim)
        dist = dist_class(model.outputs)
        self.sampler = dist.sample()

        self.variables = ray.experimental.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum([np.prod(variable.shape.as_list())
                               for _, variable
                               in self.variables.variables.items()])
        self.sess.run(tf.global_variables_initializer())

    def compute(self, observation, add_noise=False):
        action = self.sess.run(self.sampler,
                               feed_dict={self.inputs: observation})
        if add_noise:
            action += np.random.randn(*action.shape) * self.ac_noise_std
        return action

    def set_weights(self, x):
        self.variables.set_flat(x)

    def get_weights(self):
        return self.variables.get_flat()
