from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np

from ray.rllib.models.modelv2 import ModelV2, TFModelV2
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)


class ModelV1Wrapper(TFModelV2):
    """Compatibility wrapper that allows V1 models to be used as ModelV2."""

    def __init__(self, legacy_model_cls, obs_space, action_space, output_spec,
                 options, name):
        TFModelV2.__init__(self, obs_space, action_space, output_spec, options,
                           name)
        self.legacy_model_cls = legacy_model_cls

        # Tracks the last v1 model created by the call to forward
        self.cur_instance = None

        # XXX: Try to guess the initial state size. Since the size of the state
        # is known only after forward() for V1 models, it might be wrong.
        if options.get("use_lstm"):
            cell_size = options.get("lstm_cell_size", 256)
            self.initial_state = [
                np.zeros(cell_size, np.float32),
                np.zeros(cell_size, np.float32),
            ]
        else:
            self.initial_state = []

    @override(ModelV2)
    def get_initial_state(self):
        return self.initial_state

    @override(ModelV2)
    def __call__(self, input_dict, state, seq_lens):
        if self.cur_instance:
            # create a weight-sharing model copy
            with tf.variable_scope(self.cur_instance.scope, reuse=True):
                new_instance = self.legacy_model_cls(
                    input_dict, self.obs_space, self.action_space,
                    self.output_spec.size, self.options, state, seq_lens)
        else:
            # create a new model instance
            with tf.variable_scope(self.name):
                new_instance = self.legacy_model_cls(
                    input_dict, self.obs_space, self.action_space,
                    self.output_spec.size, self.options, state, seq_lens)
        self.cur_instance = new_instance
        return (new_instance.outputs, new_instance.last_layer,
                new_instance.state_out)

    @override(ModelV2)
    def get_branch_output(self, branch_type, output_spec, feature_layer=None):
        assert self.cur_instance, "must call forward first"

        # Simple case: sharing the feature layer
        if feature_layer is not None:
            with tf.variable_scope(self.cur_instance.scope):
                return tf.reshape(
                    linear(feature_layer, output_spec.size, branch_type,
                           normc_initializer(1.0)), [-1])

        # Create a new separate model with no RNN state, etc.
        branch_options = self.options.copy()
        branch_options["free_log_std"] = False
        if branch_options["use_lstm"]:
            branch_options["use_lstm"] = False
            logger.warning(
                "It is not recommended to use a LSTM model with "
                "vf_share_layers=False (consider setting it to True). "
                "If you want to not share layers, you can implement "
                "a custom LSTM model that overrides the "
                "value_function() method.")
        with tf.variable_scope(self.cur_instance.scope):
            with tf.variable_scope("branch_" + branch_type):
                branch_instance = self.legacy_model_cls(
                    self.cur_instance.input_dict,
                    self.obs_space,
                    self.action_space,
                    output_spec.size,
                    branch_options,
                    state_in=None,
                    seq_lens=None)
                return tf.reshape(branch_instance.outputs, [-1])

    @override(ModelV2)
    def custom_loss(self, policy_loss, loss_inputs):
        return self.cur_instance.custom_loss(policy_loss, loss_inputs)

    @override(ModelV2)
    def metrics(self):
        return self.cur_instance.custom_stats()

    @override(ModelV2)
    def variables(self):
        return _scope_vars(self.cur_instance.scope)


def _scope_vars(scope, trainable_only=False):
    """
    Get variables inside a scope
    The scope can be specified as a string

    Parameters
    ----------
    scope: str or VariableScope
      scope in which the variables reside.
    trainable_only: bool
      whether or not to return only the variables that were marked as
      trainable.

    Returns
    -------
    vars: [tf.Variable]
      list of variables in `scope`.
    """
    return tf.get_collection(
        tf.GraphKeys.TRAINABLE_VARIABLES
        if trainable_only else tf.GraphKeys.VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name)
