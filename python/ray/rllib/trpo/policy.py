"""Code adapted from https://github.com/mjacar/pytorch-trpo."""
from __future__ import absolute_import, division, print_function

from copy import deepcopy

import numpy as np
import ray
import torch
import torch.nn.functional as F
import torchvision.datasets as dset
import torchvision.transforms as transforms
from ray.rllib.a3c.shared_torch_policy import SharedTorchPolicy
from ray.rllib.a3c.torchpolicy import TorchPolicy
from ray.rllib.models.pytorch.misc import convert_batch, var_to_np
from ray.rllib.utils.process_rollout import discount, process_rollout
from torch import Tensor, distributions, nn
from torch.autograd import Variable
from torch.nn import BatchNorm1d, Dropout, Linear, ReLU, Sequential, Softmax
from torch.nn.utils.convert_parameters import (_check_param_device,
                                               parameters_to_vector,
                                               vector_to_parameters,)
from torch.optim import LBFGS, Adam
from torch.utils.data import DataLoader, Dataset, TensorDataset

# TODO(alok) rm unused imports

# TODO(alok) support tensorflow


def explained_variance_1d(ypred, y):
    """
    Var[ypred - y] / var[y].
    https://www.quora.com/What-is-the-meaning-proportion-of-variance-explained-in-linear-regression
    """
    assert y.ndim == 1 and ypred.ndim == 1
    vary = np.var(y)
    return np.nan if vary == 0 else 1 - np.var(y - ypred) / vary


def vector_to_gradient(v, parameters):
    # TODO(alok) may have to rm the .data from v
    r"""Convert one vector representing the
    gradient to the .grad of the parameters.

    Arguments:
        v (Tensor): a single vector represents the parameters of a model.
        parameters (Iterable[Tensor]): an iterator of Tensors that are the
            parameters of a model.
    """
    # Ensure v of type Tensor
    if not isinstance(v, torch.Tensor):
        raise TypeError('expected torch.Tensor, but got: {}'.format(
            torch.typename(v)))
    # Flag for the device where the parameter is located
    param_device = None

    # Pointer for slicing the vector for each parameter
    pointer = 0
    for param in parameters:
        # Ensure the parameters are located in the same device
        param_device = _check_param_device(param, param_device)

        # The length of the parameter
        num_param = torch.prod(torch.LongTensor(list(param.grad.size())))
        # Slice the vector, reshape it, and replace the old data of the parameter
        param.grad.data = v[pointer:pointer + num_param].view(
            param.size()).data

        # Increment the pointer
        pointer += num_param


class TRPOPolicy(SharedTorchPolicy):
    def __init__(self, *args, **kwargs):
        # TODO 
        super().__init__(*args, **kwargs)

    def mean_kl(self, policy):
        """Returns an estimate of the average KL divergence between a given
        policy and self._model."""
        new_p = policy(self.s).detach() + 1e-8
        old_p = self._model(self.s)

        return (old_p.dot((old_p / new_p).log())).mean()

    def HVP(self, v):
        """Returns the product of the Hessian of the KL divergence and the
        given vector."""

        self._model.zero_grad()

        g_kl = torch.autograd.grad(
            self.kl, self._model.parameters(), create_graph=True)
        gvp = torch.cat([grad.view(-1) for grad in g_kl]).dot(v)

        H = torch.autograd.grad(gvp, self._model.parameters())
        fisher_vector_product = torch.cat(
            [grad.contiguous().view(-1) for grad in H]).data

        return fisher_vector_product + (self.config['cg_damping'] * v.data)

    def conjugate_gradient(self, b, cg_iters=10):
        """Returns F^(-1)b where F is the Hessian of the KL divergence."""
        p = b.clone().data
        r = b.clone().data
        x = np.zeros_like(b.data.numpy())

        rdotr = r.double().dot(r.double())

        for _ in range(cg_iters):
            z = self.HVP(Variable(p)).squeeze(0)
            v = rdotr / p.double().dot(z.double())
            x += v * p.numpy()
            r -= v * z
            newrdotr = r.double().dot(r.double())
            mu = newrdotr / rdotr
            p = r + mu * p
            rdotr = newrdotr

            if rdotr < self.config['residual_tol']:
                break
        return x

    def surrogate_loss(self, params):
        """Returns the surrogate loss wrt the given parameter vector params."""

        new_policy = deepcopy(self._model)
        vector_to_parameters(params, new_policy.parameters())

        EPSILON = 1e-8

        prob_new = new_policy(self.states).gather(1, torch.cat(self.actions)).data
        prob_old = self._model(self.states).gather(
            1, torch.cat(self.actions)).data + EPSILON

        return -torch.mean((prob_new / prob_old) * self.adv)

    def linesearch(self, x, fullstep, expected_improve_rate):
        """Returns the scaled gradient that would improve the loss.

        Found via linesearch.
        """

        accept_ratio = 0.1
        max_backtracks = 10

        fval = self.surrogate_loss(x)

        for stepfrac in .5**np.arange(max_backtracks):

            g = stepfrac * fullstep

            xnew = x.data.numpy() + g
            newfval = self.surrogate_loss(Variable(torch.from_numpy(xnew)))
            actual_improve = fval - newfval
            expected_improve = expected_improve_rate * stepfrac

            if actual_improve / expected_improve > accept_ratio and actual_improve > 0:
                # return Variable(torch.from_numpy(xnew))
                return g

        # If no improvement could be obtained, return 0 gradient
        else:
            return 0

    def _backward(self, batch):
        """Fills gradient buffers up."""

        # TODO(alok) update critic as well

        states, actions, advs, rewards, _ = convert_batch(batch)
        values, action_log_probs, entropy = self._evaluate(states, actions)

        # TODO(alok) rm wrapper?

        bs = self.config['batch_size']

        # TODO(alok) add cuda

        # XXX compute_action: S -> a, prob dist

        # TODO(alok) implement in separate file or use existing implementation.
        # TODO(alok) accumulate until data is one batch big
        num_batches = len(actions) // bs if len(
            actions) % bs == 0 else (len(actions) // bs) + 1

        for batch_n in range(num_batches):
            _slice = slice(batch_n * bs, (batch_n + 1) * bs)
            self.s = states[_slice]
            self.a = actions[_slice]
            self.a_dists = action_log_probs[_slice]
            self.adv = advs[_slice]
            self.kl = self.mean_kl(self.s, self._model)

            # Calculate the surrogate loss as the element-wise product of the
            # advantage and the probability ratio of actions taken.
            # TODO(alok) Do we need log probs or the actual probabilities here?
            new_p = torch.cat(self.a_dists).gather(1, torch.cat(self.a))
            old_p = new_p.detach() + 1e-8

            prob_ratio = new_p / old_p
            surrogate_loss = -torch.mean(prob_ratio * Variable(self.adv)) - (
                self.config['ent_coeff'] * entropy)

            # Calculate the gradient of the surrogate loss
            # TODO(alok) self.policy -> self._model
            self._model.zero_grad()

            surrogate_loss.backward(retain_graph=True)

            g = parameters_to_vector(
                [v.grad for v in self._model.parameters()]).squeeze(0)

            # if g.nonzero().size()[0]:
            if g.nonzero().size():
                # Use conjugate gradient algorithm to determine the step direction in params space
                step_dir = self.conjugate_gradient(-g)
                _step_dir = Variable(torch.from_numpy(step_dir))

                # Do line search to determine the stepsize of params in the direction of step_dir
                shs = step_dir.dot(self.HVP(self.kl, _step_dir).numpy().T) / 2
                # TODO(alok) set max_kl to 0.001 as default
                lm = np.sqrt(shs / self.config['max_kl'])
                fullstep = step_dir / lm
                g_step_dir = -g.dot(_step_dir).data[0]

                grad = self.linesearch(
                    x=parameters_to_vector(self._model.parameters()),
                    fullstep=fullstep,
                    expected_improve_rate=g_step_dir / lm,
                )

                # TODO(alok) fit critic

                # Here we fill the gradient buffers
                if not any(np.isnan(grad.data.numpy())):
                    vector_to_gradient(grad, self._model.parameters())
