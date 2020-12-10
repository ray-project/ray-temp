import gym
from typing import Dict, List, Union

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.framework import try_import_jax
from ray.rllib.utils.typing import ModelConfigDict, TensorType


jax, flax = try_import_jax()
nn = None
if flax:
    import flax.linen as nn


@PublicAPI
class JAXModelV2(ModelV2, nn.Module if nn else object):
    """JAX version of ModelV2.

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):
        """Initializes a JAXModelV2 instance."""

        nn.Module.__init__(self)
        self._flax_module_variables = self.variables

        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="jax")

    @PublicAPI
    @override(ModelV2)
    def variables(self, as_dict: bool = False
                  ) -> Union[List[TensorType], Dict[str, TensorType]]:
        return self.variables

    @PublicAPI
    @override(ModelV2)
    def trainable_variables(
            self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        return self.variables
