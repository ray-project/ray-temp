from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym.spaces
import tensorflow as tf

from ray.rllib.models import ModelCatalog

def huber_loss(x, d=2.0):
    return tf.where(tf.abs(x) < d, 0.5 * tf.square(x), d*(tf.abs(x) - 0.5*d)) # condition, true, false


class ProximalPolicyLoss(object):

    def __init__(
            self, observation_space, action_space,
            observations, returns, advantages, actions, prev_logits, prev_vfpreds, logit_dim,
            kl_coeff, distribution_class, config, sess):
        assert (isinstance(action_space, gym.spaces.Discrete) or
                isinstance(action_space, gym.spaces.Box))
        self.prev_dist = distribution_class(prev_logits)

        # Saved so that we can compute actions given different observations
        self.observations = observations

        self.curr_logits = ModelCatalog.get_model(
            observations, logit_dim, config["model"]).outputs
        self.curr_dist = distribution_class(self.curr_logits)
        self.sampler = self.curr_dist.sample()

        vf_config = config["model"].copy()
        vf_config["free_logstd"] = False
        self.value_function = ModelCatalog.get_model(
            observations, 1, vf_config, "value_function").outputs
        self.value_function = tf.reshape(self.value_function, [-1])

        # Make loss functions.
        self.ratio = tf.exp(self.curr_dist.logp(actions) -
                            self.prev_dist.logp(actions))
        self.kl = self.prev_dist.kl(self.curr_dist)
        self.mean_kl = tf.reduce_mean(self.kl)
        self.entropy = self.curr_dist.entropy()
        self.mean_entropy = tf.reduce_mean(self.entropy)
        self.surr1 = self.ratio * advantages
        self.surr2 = tf.clip_by_value(self.ratio, 1 - config["clip_param"],
                                      1 + config["clip_param"]) * advantages
        self.vfloss1 = huber_loss(self.value_function - returns)
        value_function_clipped = prev_vfpreds + tf.clip_by_value(self.value_function - prev_vfpreds, -config["clip_param"], config["clip_param"])
        self.vfloss2 = huber_loss(value_function_clipped - returns)
        self.vfloss = tf.minimum(self.vfloss1, self.vfloss2)
        self.mean_vfloss = tf.reduce_mean(self.vfloss)
        self.surr = tf.minimum(self.surr1, self.surr2)
        self.mean_policyloss = tf.reduce_mean(-self.surr)
        self.loss = tf.reduce_mean(-self.surr + kl_coeff * self.kl + config["vfloss_coeff"] * self.vfloss -
                                   config["entropy_coeff"] * self.entropy)
        self.sess = sess

        if config["use_gae"]:
            self.policy_results = [self.sampler, self.curr_logits, self.value_function]
        else:
            self.policy_results = [self.sampler, self.curr_logits, tf.constant("NA")]

    def compute(self, observations):
            return self.sess.run(self.policy_results,
                                 feed_dict={self.observations: observations})

    def loss(self):
        return self.loss
