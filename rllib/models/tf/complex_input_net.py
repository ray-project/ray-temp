from gym.spaces import Box, Discrete, Tuple
import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.utils import get_filter_config
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


# __sphinx_doc_begin__
class ComplexInputNetwork(TFModelV2):
    """TFModelV2 concat'ing CNN outputs to flat input(s), followed by FC(s).

    Note: This model should be used for complex (Dict or Tuple) observation
    spaces that have one or more image components.

    The data flow is as follows:

    `obs` (e.g. Tuple[img0, img1, discrete0]) -> `CNN0 + CNN1 + ONE-HOT`
    `CNN0 + CNN1 + ONE-HOT` -> concat all flat outputs -> `out`
    `out` -> (optional) FC-stack -> `out2`
    `out2` -> action (logits) and vaulue heads.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        # TODO: (sven) Support Dicts as well.
        original_space = obs_space.original_space if \
            hasattr(obs_space, "original_space") else obs_space
        assert isinstance(original_space, (Tuple)), \
            "`obs_space.original_space` must be Tuple!"

        super().__init__(original_space, action_space, num_outputs, model_config,
                         name)

        # Build the CNN(s) given obs_space's image components.
        self.cnns = {}
        concat_size = 0
        for i, component in enumerate(original_space):
            # Image space.
            if len(component.shape) == 3:
                config = {
                    "conv_filters": model_config.get(
                        "conv_filters", get_filter_config(component.shape)),
                    "conv_activation": model_config.get("conv_activation"),
                }
                cnn = ModelCatalog.get_model_v2(
                    component,
                    action_space,
                    num_outputs=None,
                    model_config=config,
                    framework="tf",
                    name="cnn_{}".format(i))
                concat_size += cnn.num_outputs
                self.cnns[i] = cnn
            # Discrete inputs -> One-hot encode.
            elif isinstance(component, Discrete):
                concat_size += component.n
            # TODO: (sven) Multidiscrete (see e.g. our auto-LSTM wrappers).
            # Everything else (1D Box).
            else:
                assert len(component.shape) == 1, \
                    "Only input Box 1D or 3D spaces allowed!"
                concat_size += component.shape[-1]

        # Optional post-concat FC-stack.
        post_fc_stack_config = {
            "fcnet_hiddens": model_config.get("post_fcnet_hiddens", []),
            "fcnet_activation": model_config.get("post_fcnet_activation", "relu")
        }
        self.post_fc_stack = ModelCatalog.get_model_v2(Box(float("-inf"), float("inf"), shape=(concat_size, ), dtype=np.float32), self.action_space, None, post_fc_stack_config, framework="tf", name="post_fc_stack")

        # Actions and value heads.
        self.logits_and_value_model = None
        self._value_out = None
        if num_outputs:
            # Action-distribution head.
            concat_layer = tf.keras.layers.Input(
                (self.post_fc_stack.num_outputs, ))
            logits_layer = tf.keras.layers.Dense(
                num_outputs,
                activation=tf.keras.activations.linear,
                name="logits")(concat_layer)

            # Create the value branch model.
            value_layer = tf.keras.layers.Dense(
                1,
                name="value_out",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(concat_layer)
            self.logits_and_value_model = tf.keras.models.Model(
                concat_layer, [logits_layer, value_layer])
        else:
            self.num_outputs = self.post_fc_stack.num_outputs

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        # Push image observations through our CNNs.
        outs = []
        for i, component in enumerate(input_dict["obs"]):
            if i in self.cnns:
                cnn_out, _ = self.cnns[i]({"obs": component})
                outs.append(cnn_out)
            else:
                outs.append(component)
        # Concat all outputs and the non-image inputs.
        out = tf.concat(outs, axis=1)
        # Push through (optional) FC-stack (this may be an empty stack).
        out, _ = self.post_fc_stack({"obs": out}, [], None)

        # No logits/value branches.
        if not self.logits_and_value_model:
            return out, []

        # Logits- and value branches.
        logits, values = self.logits_and_value_model(out)
        self._value_out = tf.reshape(values, [-1])
        return logits, []

    @override(ModelV2)
    def value_function(self):
        return self._value_out

# __sphinx_doc_end__
