from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch.nn as nn


class Model(nn.Module):
    def __init__(self, obs_space, ac_space, options):
        super(Model, self).__init__()
        self._init(obs_space, ac_space, options)

    def _init(self, inputs, num_outputs, options):
        raise NotImplementedError

    def forward(self, obs):
        """Forward pass for the model. Internal method - should only
        be passed PyTorch Tensors.

        PyTorch automatically overloads the given model
        with this function. Recommended that model(obs)
        is used instead of model.forward(obs). See
        https://discuss.pytorch.org/t/any-different-between-model
        -input-and-model-forward-input/3690
        """
        raise NotImplementedError


class SlimConv2d(nn.Module):
    """Simple mock of tf.slim Conv2d"""

    def __init__(self, in_channels, out_channels, kernel, stride, padding):
        super(SlimConv2d, self).__init__()
        layers = []
        if padding:
            layers.append(nn.ZeroPad2d(padding))
        conv = nn.Conv2d(in_channels, out_channels, kernel, stride)
        nn.init.xavier_uniform(conv.weight)
        nn.init.constant(conv.bias, 0)
        layers += [
            conv,
            nn.ReLU()
        ]
        self._model = nn.Sequential(*layers)

    def forward(self, x):
        return self._model(x)


class Linear(nn.Module):
    """Simple PyTorch of `linear` function"""

    def __init__(self, in_size, size, initializer=None, bias_init=0):
        super(Linear, self).__init__()
        linear = nn.Linear(in_size, size)
        if initializer:
            initializer(linear.weight)
        nn.init.constant(linear.bias, bias_init)
        self._model = linear

    def forward(self, x):
        return self._model(x)
