import functools
from math import log
import numpy as np
import tree

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import SMALL_NUMBER, MIN_LOG_NN_OUTPUT, \
    MAX_LOG_NN_OUTPUT
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.torch_ops import atanh
from ray.rllib.utils.typing import TensorType, List

torch, nn = try_import_torch()


class TorchDistributionWrapper(ActionDistribution):
    """Wrapper class for torch.distributions."""

    @override(ActionDistribution)
    def __init__(self, inputs: List[TensorType], model: ModelV2):
        # If inputs are not a torch Tensor, make them one and make sure they
        # are on the correct device.
        if not isinstance(inputs, torch.Tensor):
            inputs = torch.from_numpy(inputs).to(
                next(model.parameters()).device)
        super().__init__(inputs, model)
        # Store the last sample here.
        self.last_sample = None

    @override(ActionDistribution)
    def logp(self, actions: TensorType) -> TensorType:
        return self.dist.log_prob(actions)

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        return self.dist.entropy()

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        return torch.distributions.kl.kl_divergence(self.dist, other.dist)

    @override(ActionDistribution)
    def sample(self) -> TensorType:
        self.last_sample = self.dist.sample()
        return self.last_sample

    @override(ActionDistribution)
    def sampled_action_logp(self) -> TensorType:
        assert self.last_sample is not None
        return self.logp(self.last_sample)


class TorchCategorical(TorchDistributionWrapper):
    """Wrapper class for PyTorch Categorical distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs, model=None, temperature=1.0):
        if temperature != 1.0:
            assert temperature > 0.0, \
                "Categorical `temperature` must be > 0.0!"
            inputs /= temperature
        super().__init__(inputs, model)
        self.dist = torch.distributions.categorical.Categorical(
            logits=self.inputs)

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self.dist.probs.argmax(dim=1)
        return self.last_sample

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return action_space.n


class TorchMultiCategorical(TorchDistributionWrapper):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    @override(TorchDistributionWrapper)
    def __init__(self, inputs, model, input_lens):
        super().__init__(inputs, model)
        # If input_lens is np.ndarray or list, force-make it a tuple.
        inputs_split = self.inputs.split(tuple(input_lens), dim=1)
        self.cats = [
            torch.distributions.categorical.Categorical(logits=input_)
            for input_ in inputs_split
        ]

    @override(TorchDistributionWrapper)
    def sample(self):
        arr = [cat.sample() for cat in self.cats]
        self.last_sample = torch.stack(arr, dim=1)
        return self.last_sample

    @override(ActionDistribution)
    def deterministic_sample(self):
        arr = [torch.argmax(cat.probs, -1) for cat in self.cats]
        self.last_sample = torch.stack(arr, dim=1)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def logp(self, actions):
        # # If tensor is provided, unstack it into list.
        if isinstance(actions, torch.Tensor):
            actions = torch.unbind(actions, dim=1)
        logps = torch.stack(
            [cat.log_prob(act) for cat, act in zip(self.cats, actions)])
        return torch.sum(logps, dim=0)

    @override(ActionDistribution)
    def multi_entropy(self):
        return torch.stack([cat.entropy() for cat in self.cats], dim=1)

    @override(TorchDistributionWrapper)
    def entropy(self):
        return torch.sum(self.multi_entropy(), dim=1)

    @override(ActionDistribution)
    def multi_kl(self, other):
        return torch.stack(
            [
                torch.distributions.kl.kl_divergence(cat, oth_cat)
                for cat, oth_cat in zip(self.cats, other.cats)
            ],
            dim=1,
        )

    @override(TorchDistributionWrapper)
    def kl(self, other):
        return torch.sum(self.multi_kl(other), dim=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.sum(action_space.nvec)


class TorchDiagGaussian(TorchDistributionWrapper):
    """Wrapper class for PyTorch Normal distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs, model):
        super().__init__(inputs, model)
        mean, log_std = torch.chunk(self.inputs, 2, dim=1)
        self.dist = torch.distributions.normal.Normal(mean, torch.exp(log_std))

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self.dist.mean
        return self.last_sample

    @override(TorchDistributionWrapper)
    def logp(self, actions):
        return super().logp(actions).sum(-1)

    @override(TorchDistributionWrapper)
    def entropy(self):
        return super().entropy().sum(-1)

    @override(TorchDistributionWrapper)
    def kl(self, other):
        return super().kl(other).sum(-1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape) * 2


class TorchSquashedGaussian(TorchDistributionWrapper):
    """A tanh-squashed Gaussian distribution defined by: mean, std, low, high.

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """

    def __init__(self, inputs, model, low=-1.0, high=1.0):
        """Parameterizes the distribution via `inputs`.

        Args:
            low (float): The lowest possible sampling value
                (excluding this value).
            high (float): The highest possible sampling value
                (excluding this value).
        """
        super().__init__(inputs, model)
        # Split inputs into mean and log(std).
        mean, log_std = torch.chunk(self.inputs, 2, dim=-1)
        # Clip `scale` values (coming from NN) to reasonable values.
        log_std = torch.clamp(log_std, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT)
        std = torch.exp(log_std)
        self.dist = torch.distributions.normal.Normal(mean, std)
        assert np.all(np.less(low, high))
        self.low = low
        self.high = high

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self._squash(self.dist.mean)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def sample(self):
        # Use the reparameterization version of `dist.sample` to allow for
        # the results to be backprop'able e.g. in a loss term.
        normal_sample = self.dist.rsample()
        self.last_sample = self._squash(normal_sample)
        return self.last_sample

    @override(ActionDistribution)
    def logp(self, x):
        # Unsquash values (from [low,high] to ]-inf,inf[)
        unsquashed_values = self._unsquash(x)
        # Get log prob of unsquashed values from our Normal.
        log_prob_gaussian = self.dist.log_prob(unsquashed_values)
        # For safety reasons, clamp somehow, only then sum up.
        log_prob_gaussian = torch.clamp(log_prob_gaussian, -100, 100)
        log_prob_gaussian = torch.sum(log_prob_gaussian, dim=-1)
        # Get log-prob for squashed Gaussian.
        unsquashed_values_tanhd = torch.tanh(unsquashed_values)
        log_prob = log_prob_gaussian - torch.sum(
            torch.log(1 - unsquashed_values_tanhd**2 + SMALL_NUMBER), dim=-1)
        return log_prob

    def _squash(self, raw_values):
        # Returned values are within [low, high] (including `low` and `high`).
        squashed = ((torch.tanh(raw_values) + 1.0) / 2.0) * \
            (self.high - self.low) + self.low
        return torch.clamp(squashed, self.low, self.high)

    def _unsquash(self, values):
        normed_values = (values - self.low) / (self.high - self.low) * 2.0 - \
                        1.0
        # Stabilize input to atanh.
        save_normed_values = torch.clamp(normed_values, -1.0 + SMALL_NUMBER,
                                         1.0 - SMALL_NUMBER)
        unsquashed = atanh(save_normed_values)
        return unsquashed

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape) * 2


class TorchBeta(TorchDistributionWrapper):
    """
    A Beta distribution is defined on the interval [0, 1] and parameterized by
    shape parameters alpha and beta (also called concentration parameters).

    PDF(x; alpha, beta) = x**(alpha - 1) (1 - x)**(beta - 1) / Z
        with Z = Gamma(alpha) Gamma(beta) / Gamma(alpha + beta)
        and Gamma(n) = (n - 1)!
    """

    def __init__(self, inputs, model, low=0.0, high=1.0):
        super().__init__(inputs, model)
        # Stabilize input parameters (possibly coming from a linear layer).
        self.inputs = torch.clamp(self.inputs, log(SMALL_NUMBER),
                                  -log(SMALL_NUMBER))
        self.inputs = torch.log(torch.exp(self.inputs) + 1.0) + 1.0
        self.low = low
        self.high = high
        alpha, beta = torch.chunk(self.inputs, 2, dim=-1)
        # Note: concentration0==beta, concentration1=alpha (!)
        self.dist = torch.distributions.Beta(
            concentration1=alpha, concentration0=beta)

    @override(ActionDistribution)
    def deterministic_sample(self):
        self.last_sample = self._squash(self.dist.mean)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def sample(self):
        # Use the reparameterization version of `dist.sample` to allow for
        # the results to be backprop'able e.g. in a loss term.
        normal_sample = self.dist.rsample()
        self.last_sample = self._squash(normal_sample)
        return self.last_sample

    @override(ActionDistribution)
    def logp(self, x):
        unsquashed_values = self._unsquash(x)
        return torch.sum(self.dist.log_prob(unsquashed_values), dim=-1)

    def _squash(self, raw_values):
        return raw_values * (self.high - self.low) + self.low

    def _unsquash(self, values):
        return (values - self.low) / (self.high - self.low)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape) * 2


class TorchDeterministic(TorchDistributionWrapper):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).
    """

    @override(ActionDistribution)
    def deterministic_sample(self):
        return self.inputs

    @override(TorchDistributionWrapper)
    def sampled_action_logp(self):
        return torch.zeros((self.inputs.size()[0], ), dtype=torch.float32)

    @override(TorchDistributionWrapper)
    def sample(self):
        return self.deterministic_sample()

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape)


class TorchMultiActionDistribution(TorchDistributionWrapper):
    """Action distribution that operates on multiple, possibly nested actions.
    """

    def __init__(self, inputs, model, *, child_distributions, input_lens,
                 action_space):
        """Initializes a TorchMultiActionDistribution object.

        Args:
            inputs (torch.Tensor): A single tensor of shape [BATCH, size].
            model (ModelV2): The ModelV2 object used to produce inputs for this
                distribution.
            child_distributions (any[torch.Tensor]): Any struct
                that contains the child distribution classes to use to
                instantiate the child distributions from `inputs`. This could
                be an already flattened list or a struct according to
                `action_space`.
            input_lens (any[int]): A flat list or a nested struct of input
                split lengths used to split `inputs`.
            action_space (Union[gym.spaces.Dict,gym.spaces.Tuple]): The complex
                and possibly nested action space.
        """
        if not isinstance(inputs, torch.Tensor):
            inputs = torch.Tensor(inputs)
        super().__init__(inputs, model)

        self.action_space_struct = get_base_struct_from_space(action_space)

        self.input_lens = tree.flatten(input_lens)
        flat_child_distributions = tree.flatten(child_distributions)
        split_inputs = torch.split(inputs, self.input_lens, dim=1)
        self.flat_child_distributions = tree.map_structure(
            lambda dist, input_: dist(input_, model), flat_child_distributions,
            list(split_inputs))

    @override(ActionDistribution)
    def logp(self, x):
        if isinstance(x, np.ndarray):
            x = torch.Tensor(x)
        # Single tensor input (all merged).
        if isinstance(x, torch.Tensor):
            split_indices = []
            for dist in self.flat_child_distributions:
                if isinstance(dist, TorchCategorical):
                    split_indices.append(1)
                else:
                    split_indices.append(dist.sample().size()[1])
            split_x = list(torch.split(x, split_indices, dim=1))
        # Structured or flattened (by single action component) input.
        else:
            split_x = tree.flatten(x)

        def map_(val, dist):
            # Remove extra categorical dimension.
            if isinstance(dist, TorchCategorical):
                val = torch.squeeze(val, dim=-1).int()
            return dist.logp(val)

        # Remove extra categorical dimension and take the logp of each
        # component.
        flat_logps = tree.map_structure(map_, split_x,
                                        self.flat_child_distributions)

        return functools.reduce(lambda a, b: a + b, flat_logps)

    @override(ActionDistribution)
    def kl(self, other):
        kl_list = [
            d.kl(o) for d, o in zip(self.flat_child_distributions,
                                    other.flat_child_distributions)
        ]
        return functools.reduce(lambda a, b: a + b, kl_list)

    @override(ActionDistribution)
    def entropy(self):
        entropy_list = [d.entropy() for d in self.flat_child_distributions]
        return functools.reduce(lambda a, b: a + b, entropy_list)

    @override(ActionDistribution)
    def sample(self):
        child_distributions = tree.unflatten_as(self.action_space_struct,
                                                self.flat_child_distributions)
        return tree.map_structure(lambda s: s.sample(), child_distributions)

    @override(ActionDistribution)
    def deterministic_sample(self):
        child_distributions = tree.unflatten_as(self.action_space_struct,
                                                self.flat_child_distributions)
        return tree.map_structure(lambda s: s.deterministic_sample(),
                                  child_distributions)

    @override(TorchDistributionWrapper)
    def sampled_action_logp(self):
        p = self.flat_child_distributions[0].sampled_action_logp()
        for c in self.flat_child_distributions[1:]:
            p += c.sampled_action_logp()
        return p

    @override(ActionDistribution)
    def required_model_output_shape(self, action_space, model_config):
        return np.sum(self.input_lens)
