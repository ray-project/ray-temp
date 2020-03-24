from gym.spaces import Discrete
import numpy as np

from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class SACTorchModel(TorchModelV2, nn.Module):
    """Extension of standard TorchModelV2 for SAC.

    Data flow:
        obs -> forward() -> model_out
        model_out -> get_policy_output() -> pi(s)
        model_out, actions -> get_q_values() -> Q(s, a)
        model_out, actions -> get_twin_q_values() -> Q_twin(s, a)

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 actor_hidden_activation="relu",
                 actor_hiddens=(256, 256),
                 critic_hidden_activation="relu",
                 critic_hiddens=(256, 256),
                 twin_q=False,
                 initial_alpha=1.0):
        """Initialize variables of this model.

        Extra model kwargs:
            actor_hidden_activation (str): activation for actor network
            actor_hiddens (list): hidden layers sizes for actor network
            critic_hidden_activation (str): activation for critic network
            critic_hiddens (list): hidden layers sizes for critic network
            twin_q (bool): build twin Q networks.
            initial_alpha (float): The initial value for the to-be-optimized
                alpha parameter (default: 1.0).

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the output heads. Those layers for
        forward() should be defined in subclasses of SACModel.
        """
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        self.discrete = False
        if isinstance(action_space, Discrete):
            self.action_dim = action_space.n
            self.discrete = True
            self.action_outs = q_outs = self.action_dim
            self.action_ins = None  # No action inputs for the discrete case.
        else:
            self.action_dim = np.product(action_space.shape)
            self.action_outs = 2 * self.action_dim
            self.action_ins = self.action_dim
            q_outs = 1

        # Build the policy network.
        self.action_model = nn.Sequential()
        ins = obs_space.shape[-1]
        self.obs_ins = ins
        for i, n in enumerate(actor_hiddens):
            self.action_model.add_module("action_{}".format(i), nn.Linear(ins, n))
            # Add activations if necessary.
            if actor_hidden_activation == "relu":
                self.action_model.add_module("action_activation_{}".format(i), nn.ReLU())
            elif actor_hidden_activation == "tanh":
                self.action_model.add_module("action_activation_{}".format(i), nn.Tanh())
            ins = n
        self.action_model.add_module("action_out", nn.Linear(ins, self.action_outs))
        #self.action_model = nn.ModuleList(self.action_model)

        # Build the Q-net(s), including target Q-net(s).
        def build_q_net(name):
            # For continuous actions: Feed obs and actions (concatenated)
            # through the NN. For discrete actions, only obs.
            q_net = nn.Sequential()
            ins = self.obs_ins + (0 if self.discrete else self.action_ins)
            for i, n in enumerate(critic_hiddens):
                q_net.add_module("{}_hidden_{}".format(name, i), nn.Linear(ins, n))
                # Add activations if necessary.
                if critic_hidden_activation == "relu":
                    q_net.add_module("{}_activation_{}".format(name, i), nn.ReLU())
                elif critic_hidden_activation == "tanh":
                    q_net.add_module("{}_activation_{}".format(name, i), nn.Tanh())
                ins = n

            q_net.add_module("{}_out".format(name), nn.Linear(ins, q_outs))
            #q_net = nn.ModuleList(q_net)
            return q_net

        self.q_net = build_q_net("q")
        if twin_q:
            self.twin_q_net = build_q_net("twin_q")
        else:
            self.twin_q_net = None

        self.log_alpha = torch.Tensor([np.log(initial_alpha)]).float()
        self.alpha = torch.exp(self.log_alpha)
        
        #self.network = nn.ModuleList([
        #    self.shared_module, self.advantage_module, self.value_module])

    def get_q_values(self, model_out, actions=None):
        """Return the Q estimates for the most recent forward pass.

        This implements Q(s, a).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim]. If None (discrete action
                case), return Q-values for all actions.

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        if actions is not None:
            return self.q_net(torch.cat([model_out, actions], -1))
        else:
            return self.q_net(model_out)

    def get_twin_q_values(self, model_out, actions=None):
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Optional[Tensor]): Actions to return the Q-values for.
                Shape: [BATCH_SIZE, action_dim]. If None (discrete action
                case), return Q-values for all actions.

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        if actions is not None:
            return self.twin_q_net(torch.cat([model_out, actions], -1))
        else:
            return self.twin_q_net(model_out)

    def get_policy_output(self, model_out):
        """Return the action output for the most recent forward pass.

        This outputs the support for pi(s). For continuous action spaces, this
        is the action directly. For discrete, is is the mean / std dev.

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].

        Returns:
            tensor of shape [BATCH_SIZE, action_out_size]
        """
        return self.action_model(model_out)

    def policy_variables(self):
        """Return the list of variables for the policy net."""

        return list(self.action_model.parameters())

    def q_variables(self):
        """Return the list of variables for Q / twin Q nets."""

        return list(self.q_net.parameters()) + \
               (list(self.twin_q_net.parameters()) if self.twin_q_net else [])
