import numpy as np

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.modules.noisy_layer import NoisyLayer
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class DQNTorchModel(TorchModelV2, nn.Module):
    """Extension of standard TorchModelV2 to provide dueling-Q functionality.
    """

    def __init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            *,
            q_hiddens=(256, ),
            dueling=False,
            dueling_activation="relu",
            num_atoms=1,
            use_noisy=False,
            sigma0=0.5,
            # TODO(sven): Move `add_layer_norm` into ModelCatalog as
            #  generic option, then error if we use ParameterNoise as
            #  Exploration type and do not have any LayerNorm layers in
            #  the net.
            add_layer_norm=False):
        """Initialize variables of this model.

        Extra model kwargs:
            q_hiddens (List[int]): List of layer-sizes after(!) the
                Advantages(A)/Value(V)-split. Hence, each of the A- and V-
                branches will have this structure of Dense layers. To define
                the NN before this A/V-split, use - as always -
                config["model"]["fcnet_hiddens"].
            dueling (bool): Whether to build the advantage(A)/value(V) heads
                for DDQN. If True, Q-values are calculated as:
                Q = (A - mean[A]) + V. If False, raw NN output is interpreted
                as Q-values.
            dueling_activation (str): The activation to use for all dueling
                layers (A- and V-branch). One of "relu", "tanh", "linear".
            num_atoms (int): if >1, enables distributional DQN
            use_noisy (bool): use noisy nets
            sigma0 (float): initial value of noisy nets
            add_layer_norm (bool): Enable layer norm (for param noise).
        """
        nn.Module.__init__(self)
        super(DQNTorchModel, self).__init__(obs_space, action_space,
                                            num_outputs, model_config, name)

        self.dueling = dueling
        ins = num_outputs

        advantage_module = nn.Sequential()
        value_module = nn.Sequential()

        # Dueling case: Build the shared (advantages and value) fc-network.
        for i, n in enumerate(q_hiddens):
            advantage_module.add_module("dueling_A_{}".format(i),
                                        nn.Linear(ins, n))
            value_module.add_module("dueling_V_{}".format(i),
                                    nn.Linear(ins, n))
            # Add activations if necessary.
            if dueling_activation == "relu":
                advantage_module.add_module("dueling_A_act_{}".format(i),
                                            nn.ReLU())
                value_module.add_module("dueling_V_act_{}".format(i),
                                        nn.ReLU())
            elif dueling_activation == "tanh":
                advantage_module.add_module("dueling_A_act_{}".format(i),
                                            nn.Tanh())
                value_module.add_module("dueling_V_act_{}".format(i),
                                        nn.Tanh())

            # Add LayerNorm after each Dense.
            if add_layer_norm:
                advantage_module.add_module("LayerNorm_A_{}".format(i),
                                            nn.LayerNorm(n))
                value_module.add_module("LayerNorm_V_{}".format(i),
                                        nn.LayerNorm(n))
            ins = n

        # Actual Advantages layer (nodes=num-actions).
        if q_hiddens:
            advantage_module.add_module("A", nn.Linear(ins, action_space.n))

        self.advantage_module = advantage_module

        # Value layer (nodes=1).
        if self.dueling:
            value_module.add_module("V", nn.Linear(ins, 1))
            self.value_module = value_module

    def get_advantages_or_q_values(self, model_out):
        """Returns distributional values for Q(s, a) given a state embedding.

        Override this in your custom model to customize the Q output head.

        Arguments:
            model_out (Tensor): embedding from the model layers

        Returns:
            (action_scores, logits, dist) if num_atoms == 1, otherwise
            (action_scores, z, support_logits_per_action, logits, dist)
        """

        return self.advantage_module(model_out)

    def get_state_value(self, model_out):
        """Returns the state value prediction for the given state embedding."""

        return self.value_module(model_out)
