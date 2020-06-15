import numpy as np

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.misc import normc_initializer, same_padding, \
    SlimConv2d, SlimFC
from ray.rllib.models.tf.visionnet_v1 import _get_filter_config
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import get_activation_fn
from ray.rllib.utils import try_import_torch

_, nn = try_import_torch()


class VisionNetwork(TorchModelV2, nn.Module):
    """Generic vision network."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        activation = get_activation_fn(
            model_config.get("conv_activation"), framework="torch")
        filters = model_config.get("conv_filters")
        if not filters:
            filters = _get_filter_config(obs_space.shape)
        no_final_linear = model_config.get("no_final_linear")
        vf_share_layers = model_config.get("vf_share_layers")

        self.last_layer_is_flattened = False
        self._logits = None

        layers = []
        (w, h, in_channels) = obs_space.shape
        in_size = [w, h]
        for out_channels, kernel, stride in filters[:-1]:
            padding, out_size = same_padding(in_size, kernel,
                                             [stride, stride])
            layers.append(
                SlimConv2d(
                    in_channels,
                    out_channels,
                    kernel,
                    stride,
                    padding,
                    activation_fn=activation))
            in_channels = out_channels
            in_size = out_size

        out_channels, kernel, stride = filters[-1]

        # No final linear: Last layer is a Conv2D and uses num_outputs.
        if no_final_linear and num_outputs:
            layers.append(
                SlimConv2d(
                    in_channels,
                    num_outputs,
                    kernel,
                    stride,
                    None,
                    activation_fn=activation))
        # Finish network normally (w/o overriding last layer size with
        # `num_outputs`), then add another linear one of size `num_outputs`.
        else:
            layers.append(SlimConv2d(
                in_channels,
                out_channels,
                kernel,
                stride,
                None,
                activation_fn=activation))

            if num_outputs:
                in_size = [
                    np.ceil((in_size[0] - kernel[0]) / stride),
                    np.ceil((in_size[1] - kernel[1]) / stride)]
                padding, _ = same_padding(in_size, [1, 1], [1, 1])
                self._logits = SlimConv2d(
                    out_channels,
                    num_outputs,
                    [1, 1],
                    1,
                    padding,
                    activation_fn=None)
            else:
                self.last_layer_is_flattened = True
                layers.append(nn.Flatten())
                self.num_outputs = out_channels

        self._convs = nn.Sequential(*layers)

        #self._logits = SlimFC(
        #    out_channels, num_outputs, initializer=nn.init.xavier_uniform_)
        self._value_branch = SlimFC(
            out_channels, 1, initializer=normc_initializer())
        # Holds the current "base" output (before logits layer).
        self._features = None

    @override(TorchModelV2)
    def forward(self, input_dict, state, seq_lens):
        self._features = self._convs(input_dict["obs"].float().permute(0, 3, 1, 2))  #self._hidden_layers(input_dict["obs"].float())
        #self._features = self._last_conv2d(self._features)
        if not self.last_layer_is_flattened:
            logits = self._logits(self._features)
            logits = logits.squeeze(3)
            logits = logits.squeeze(2)
            return logits, state

        #test = self._flatten(self._features)
        #return test, state
        return self._features, state

    @override(TorchModelV2)
    def value_function(self):
        assert self._features is not None, "must call forward() first"
        return self._value_branch(self._features).squeeze(1)

    def _hidden_layers(self, obs):
        res = self._convs(obs.permute(0, 3, 1, 2))  # switch to channel-major
        res = res.squeeze(3)
        res = res.squeeze(2)
        return res
