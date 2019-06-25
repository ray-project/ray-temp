"""Example of using a custom ModelV2 Keras-style model.

TODO(ekl): add this to docs once ModelV2 is fully implemented.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse

import ray
from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.agents.dqn.simple_q_model import SimpleQModel
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="DQN")  # Try PG, PPO, DQN
parser.add_argument("--stop", type=int, default=200)


class MyKerasModel(TFModelV2):
    """Custom model for policy gradient algorithms."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(MyKerasModel, self).__init__(obs_space, action_space,
                                           num_outputs, model_config, name)
        self.inputs = tf.keras.layers.Input(
            shape=obs_space.shape, name="observations")
        layer_1 = tf.keras.layers.Dense(
            128,
            name="my_layer1",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0))(self.inputs)
        layer_out = tf.keras.layers.Dense(
            num_outputs,
            name="my_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(layer_1)
        value_out = tf.keras.layers.Dense(
            1,
            name="value_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(layer_1)
        self.base_model = tf.keras.Model(self.inputs, [layer_out, value_out])
        self.register_variables(self.base_model.variables)

    def forward(self, input_dict, state, seq_lens):
        self.prev_input = input_dict
        model_out, self._value_out = self.base_model(input_dict["obs"])
        return model_out, state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])


class MyKerasQModel(SimpleQModel):
    """Custom model for DQN."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, q_hiddens):
        super(MyKerasQModel,
              self).__init__(obs_space, action_space, num_outputs,
                             model_config, name, q_hiddens)
        self.inputs = tf.keras.layers.Input(
            shape=obs_space.shape, name="observations")
        layer_1 = tf.keras.layers.Dense(
            128,
            name="my_layer1",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0))(self.inputs)
        layer_out = tf.keras.layers.Dense(
            num_outputs,
            name="my_out",
            activation=tf.nn.relu,
            kernel_initializer=normc_initializer(1.0))(layer_1)
        self.base_model = tf.keras.Model(self.inputs, layer_out)
        self.register_variables(self.base_model.variables)

    def forward(self, input_dict, state, seq_lens):
        self.prev_input = input_dict
        model_out = self.base_model(input_dict["obs"])
        return model_out, state

    def get_q_values(self, model_out):
        # using default impl from SimpleQModel
        return self.q_value_head(model_out)


if __name__ == "__main__":
    ray.init(local_mode=True)
    args = parser.parse_args()
    ModelCatalog.register_custom_model("keras_model", MyKerasModel)
    ModelCatalog.register_custom_model("keras_q_model", MyKerasQModel)
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": "CartPole-v0",
            "model": {
                "custom_model": "keras_q_model"
                if args.run == "DQN" else "keras_model"
            },
        })
