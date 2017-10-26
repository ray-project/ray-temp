from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import ray
import gym


class Policy(object):
    """The policy base class."""
    def __init__(self, ob_space, action_space, name="local", summarize=True):
        pass

    def apply_gradients(self, grads):
        raise NotImplementedError

    def get_weights(self):
        raise NotImplementedError

    def set_weights(self, weights):
        raise NotImplementedError

    def get_gradients(self, batch):
        raise NotImplementedError

    def get_vf_loss(self):
        raise NotImplementedError

    def compute_actions(self, observations):
        raise NotImplementedError

    def value(self, ob):
        raise NotImplementedError
