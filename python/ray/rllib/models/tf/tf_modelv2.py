from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.modelv2 import ModelV2


class TFModelV2(ModelV2):
    """TF version of ModelV2."""

    def __init__(self, obs_space, action_space, output_spec, options, name):
        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            output_spec,
            options,
            name,
            framework="tf")
