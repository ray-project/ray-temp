import logging
import numpy as np
import time
from typing import Dict, List, Union

from ray.rllib.models.jax.jax_modelv2 import JAXModelV2
from ray.rllib.models.jax.modules.fc_stack import FCStack
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.utils.typing import TensorType

jax, flax = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp

logger = logging.getLogger(__name__)


class FullyConnectedNetwork(JAXModelV2):
    """Generic fully connected network."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        activation = model_config.get("fcnet_activation")
        hiddens = model_config.get("fcnet_hiddens", [])
        no_final_linear = model_config.get("no_final_linear")
        in_features = int(np.product(obs_space.shape))
        self.vf_share_layers = model_config.get("vf_share_layers")
        self.free_log_std = model_config.get("free_log_std")

        if self.free_log_std:
            raise ValueError("`free_log_std` not supported for JAX yet!")

        self._logits = None
        self._logits_params = None

        # The last layer is adjusted to be of size num_outputs, but it's a
        # layer with activation.
        if no_final_linear and num_outputs:
            self._hidden_layers = FCStack(
                in_features=in_features,
                layers=hiddens + [num_outputs],
                activation=activation,
                prng_key=self.prng_key,
            )

        # Finish the layers with the provided sizes (`hiddens`), plus -
        # iff num_outputs > 0 - a last linear layer of size num_outputs.
        else:
            prev_layer_size = in_features
            if len(hiddens) > 0:
                self._hidden_layers = FCStack(
                    in_features=prev_layer_size,
                    layers=hiddens,
                    activation=activation,
                    prng_key=self.prng_key,
                )
                prev_layer_size = hiddens[-1]
            if num_outputs:
                self._logits = FCStack(
                    in_features=prev_layer_size,
                    layers=[num_outputs],
                    activation=None,
                    prng_key=self.prng_key,
                )
                self._logits_params = self._logits.init(self.prng_key, jnp.zeros((1, prev_layer_size)))
            else:
                self.num_outputs = (
                    [int(np.product(obs_space.shape))] + hiddens[-1:])[-1]

        # Init hidden layers.
        in_ = jnp.zeros((1, in_features))
        self._hidden_layers_params = self._hidden_layers.init(self.prng_key, in_)

        self._value_branch_separate = None
        if not self.vf_share_layers:
            # Build a parallel set of hidden layers for the value net.
            self._value_branch_separate = FCStack(
                in_features=int(np.product(obs_space.shape)),
                layers=hiddens,
                activation=activation,
                prng_key=self.prng_key,
            )
        self._value_branch = FCStack(
            in_features=prev_layer_size,
            layers=[1],
            prng_key=self.prng_key,
        )
        # Holds the current "base" output (before logits layer).
        self._features = None
        # Holds the last input, in case value branch is separate.
        self._last_flat_in = None

    @override(JAXModelV2)
    def forward(self, input_dict, state, seq_lens):
        self._last_flat_in = input_dict["obs_flat"]
        self._features = self._hidden_layers.apply(self._hidden_layers_params, self._last_flat_in)
        logits = self._logits.apply(self._logits_params, self._features) if self._logits else \
            self._features
        return logits, state

    @override(JAXModelV2)
    def value_function(self):
        assert self._features is not None, "must call forward() first"
        if self._value_branch_separate:
            x = self._value_branch_separate(self._last_flat_in)
            return self._value_branch(x).squeeze(1)
        else:
            return self._value_branch(self._features).squeeze(1)
