from ray.rllib.a3c.torchpolicy import TorchPolicy
from ray.rllib.models.pytorch.misc import var_to_np, convert_batch
from ray.rllib.models.catalog import ModelCatalog


import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F


class SharedTorchModel(TorchPolicy):
    """Assumes nonrecurrent."""

    def __init__(self, ob_space, ac_space, **kwargs):
        super(SharedTorchModel, self).__init__(
            ob_space, ac_space, **kwargs)

    def _setup_graph(self, ob_space, ac_space):
        _, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = ModelCatalog.get_torch_model(ob_space, self.logit_dim)
        self.optimizer = torch.optim.SGD(self._model.parameters(), lr=0.001)

    def compute_action(self, x, *args):
        x = Variable(torch.from_numpy(x).float())
        logits, values, features = self._model(x)
        samples = self._model.probs(logits.unsqueeze(0)).multinomial().squeeze()
        return var_to_np(samples), var_to_np(values), features

    def compute_logits(self, x, *args):
        x = Variable(torch.from_numpy(x).float())
        res = self._model.hidden_layers(x)
        return var_to_np(self._model.logits(res))

    def value(self, x, *args):
        x = Variable(torch.from_numpy(x).float())
        res = self._model.hidden_layers(x)
        res = self._model.value_branch(res)
        return var_to_np(res)

    def _evaluate(self, x, actions):
        logits, values, features = self._model(x)
        log_probs = F.log_softmax(logits)
        probs = self._model.probs(logits)
        action_log_probs = log_probs.gather(1, actions.view(-1, 1))
        entropy = -(log_probs * probs).sum(-1).mean()
        return values, action_log_probs, entropy

    def _backward(self, batch):
        """Loss is encoded in here. Defining a new loss function
        would start by rewriting this function"""

        states, acs, advs, rs, _ = convert_batch(batch)
        values, ac_logprobs, entropy = self._evaluate(states, acs)
        pi_err = -(advs * ac_logprobs).mean()
        value_err = (values - rs).pow(2).mean()

        self.optimizer.zero_grad()
        overall_err = value_err + pi_err - entropy * 0.1
        overall_err.backward()

    def get_initial_features(self):
        return [None]
