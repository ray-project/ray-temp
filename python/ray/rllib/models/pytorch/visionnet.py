from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F

from ray.rllib.models.pytorch.model import Model


class VisionNetwork(Model):
    """Generic vision network."""

    def _init(self, inputs, num_outputs, options):
        filters = options.get("conv_filters", [
            [16, 8, 4],
            [32, 4, 2]
        ])
        layers = []
        input_channels = inputs[0]
        for out_size, kernel, stride in filters:
            layers.append(nn.Conv2d(
                input_channels, out_size, kernel, stride))
            input_channels = out_size

        out_size = 512
        self._convs = nn.Sequential(*layers)

        # TODO(rliaw): This should definitely not be hardcoded
        self.fc1 = nn.Linear(32*8*8, out_size)
        self.logits = nn.Linear(out_size, num_outputs)
        self.probs = nn.Softmax()
        self.value_branch = nn.Linear(out_size, 1)

    def hidden_layers(self, inputs):
        """ Internal method - pass in Variables, not numpy arrays

        args:
            inputs: observations and features"""
        res = self._convs(inputs)
        res = res.view(-1, 32*8*8)
        return self.fc1(res)

    def forward(self, inputs):
        """ Internal method - pass in Variables, not numpy arrays

        args:
            inputs: observations and features"""
        res = self.hidden_layers(inputs)
        logits = self.logits(res)
        value = self.value_branch(res)
        return logits, value, None

