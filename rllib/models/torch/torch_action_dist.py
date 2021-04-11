import functools
from math import log
import numpy as np
import tree
import gym

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import SMALL_NUMBER, MIN_LOG_NN_OUTPUT, \
    MAX_LOG_NN_OUTPUT
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.torch_ops import atanh
from ray.rllib.utils.typing import TensorType, List, Union, \
    Tuple, ModelConfigDict

torch, nn = try_import_torch()


class TorchDistributionWrapper(ActionDistribution):
    """Wrapper class for torch.distributions."""

    @override(ActionDistribution)
    def __init__(self, inputs: List[TensorType], model: TorchModelV2):
        # If inputs are not a torch Tensor, make them one and make sure they
        # are on the correct device.
        if not isinstance(inputs, torch.Tensor):
            inputs = torch.from_numpy(inputs)
            if isinstance(model, TorchModelV2):
                inputs = inputs.to(next(model.parameters()).device)
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
    def __init__(self,
                 inputs: List[TensorType],
                 model: TorchModelV2 = None,
                 temperature: float = 1.0):
        if temperature != 1.0:
            assert temperature > 0.0, \
                "Categorical `temperature` must be > 0.0!"
            inputs /= temperature
        super().__init__(inputs, model)
        self.dist = torch.distributions.categorical.Categorical(
            logits=self.inputs)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        self.last_sample = self.dist.probs.argmax(dim=1)
        return self.last_sample

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        return action_space.n


class TorchMultiCategorical(TorchDistributionWrapper):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    @override(TorchDistributionWrapper)
    def __init__(self,
                 inputs: List[TensorType],
                 model: TorchModelV2,
                 input_lens: Union[List[int], np.ndarray, Tuple[int, ...]],
                 action_space=None):
        super().__init__(inputs, model)
        # If input_lens is np.ndarray or list, force-make it a tuple.
        inputs_split = self.inputs.split(tuple(input_lens), dim=1)
        self.cats = [
            torch.distributions.categorical.Categorical(logits=input_)
            for input_ in inputs_split
        ]
        # Used in case we are dealing with an Int Box.
        self.action_space = action_space

    @override(TorchDistributionWrapper)
    def sample(self) -> TensorType:
        arr = [cat.sample() for cat in self.cats]
        sample_ = torch.stack(arr, dim=1)
        if isinstance(self.action_space, gym.spaces.Box):
            sample_ = torch.reshape(sample_,
                                    [-1] + list(self.action_space.shape))
        self.last_sample = sample_
        return sample_

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        arr = [torch.argmax(cat.probs, -1) for cat in self.cats]
        sample_ = torch.stack(arr, dim=1)
        if isinstance(self.action_space, gym.spaces.Box):
            sample_ = torch.reshape(sample_,
                                    [-1] + list(self.action_space.shape))
        self.last_sample = sample_
        return sample_

    @override(TorchDistributionWrapper)
    def logp(self, actions: TensorType) -> TensorType:
        # # If tensor is provided, unstack it into list.
        if isinstance(actions, torch.Tensor):
            if isinstance(self.action_space, gym.spaces.Box):
                actions = torch.reshape(
                    actions, [-1, int(np.product(self.action_space.shape))])
            actions = torch.unbind(actions, dim=1)
        logps = torch.stack(
            [cat.log_prob(act) for cat, act in zip(self.cats, actions)])
        return torch.sum(logps, dim=0)

    @override(ActionDistribution)
    def multi_entropy(self) -> TensorType:
        return torch.stack([cat.entropy() for cat in self.cats], dim=1)

    @override(TorchDistributionWrapper)
    def entropy(self) -> TensorType:
        return torch.sum(self.multi_entropy(), dim=1)

    @override(ActionDistribution)
    def multi_kl(self, other: ActionDistribution) -> TensorType:
        return torch.stack(
            [
                torch.distributions.kl.kl_divergence(cat, oth_cat)
                for cat, oth_cat in zip(self.cats, other.cats)
            ],
            dim=1,
        )

    @override(TorchDistributionWrapper)
    def kl(self, other: ActionDistribution) -> TensorType:
        return torch.sum(self.multi_kl(other), dim=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        # Int Box.
        if isinstance(action_space, gym.spaces.Box):
            assert action_space.dtype.name.startswith("int")
            low_ = np.min(action_space.low)
            high_ = np.max(action_space.high)
            assert np.all(action_space.low == low_)
            assert np.all(action_space.high == high_)
            np.product(action_space.shape) * (high_ - low_ + 1)
        # MultiDiscrete space.
        else:
            return np.sum(action_space.nvec)


class TorchDiagGaussian(TorchDistributionWrapper):
    """Wrapper class for PyTorch Normal distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs: List[TensorType], model: TorchModelV2):
        super().__init__(inputs, model)
        mean, log_std = torch.chunk(self.inputs, 2, dim=1)
        self.dist = torch.distributions.normal.Normal(mean, torch.exp(log_std))

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        self.last_sample = self.dist.mean
        return self.last_sample

    @override(TorchDistributionWrapper)
    def logp(self, actions: TensorType) -> TensorType:
        return super().logp(actions).sum(-1)

    @override(TorchDistributionWrapper)
    def entropy(self) -> TensorType:
        return super().entropy().sum(-1)

    @override(TorchDistributionWrapper)
    def kl(self, other: ActionDistribution) -> TensorType:
        return super().kl(other).sum(-1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape) * 2


class _TorchSquashedGaussianBase(TorchDistributionWrapper):
    """A diagonal gaussian distribution, squashed into bounded support."""

    def __init__(self,
                 inputs: List[TensorType],
                 model: TorchModelV2,
                 low: float = -1.0,
                 high: float = 1.0):
        """Parameterizes the distribution via `inputs`.

        Args:
            low (float): The lowest possible sampling value
                (excluding this value).
            high (float): The highest possible sampling value
                (excluding this value).
        """
        super().__init__(inputs, model)

        assert low < high
        # Make sure high and low are torch tensors.
        self.low = torch.from_numpy(np.array(low))
        self.high = torch.from_numpy(np.array(high))
        # Place on correct device.
        if isinstance(model, TorchModelV2):
            device = next(model.parameters()).device
            self.low = self.low.to(device)
            self.high = self.high.to(device)

        mean, log_std = torch.chunk(self.inputs, 2, dim=-1)
        self._num_vars = mean.shape[1]
        assert log_std.shape[1] == self._num_vars
        # Clip `std` values (coming from NN) to reasonable values.
        self.log_std = torch.clamp(log_std, MIN_LOG_NN_OUTPUT,
                                   MAX_LOG_NN_OUTPUT)
        # Clip loc too, for numerical stability reasons.
        mean = torch.clamp(mean, -3, 3)
        std = torch.exp(self.log_std)
        self.distr = torch.distributions.normal.Normal(mean, std)
        assert len(self.distr.loc.shape) == 2
        assert len(self.distr.scale.shape) == 2

    @override(TorchDistributionWrapper)
    def sample(self):
        s = self._squash(self.distr.sample())
        assert len(s.shape) == 2
        self.last_sample = s
        return s

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        mean = self.distr.loc
        assert len(mean.shape) == 2
        s = self._squash(mean)
        assert len(s.shape) == 2
        self.last_sample = s
        return s

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        # Unsquash values (from [low,high] to ]-inf,inf[)
        assert len(x.shape) >= 2, "First dim batch, second dim variable"
        unsquashed_values = self._unsquash(x)
        # Get log prob of unsquashed values from our Normal.
        log_prob_gaussian = self.distr.log_prob(unsquashed_values)
        # For safety reasons, clamp somehow, only then sum up.
        log_prob_gaussian = torch.clamp(log_prob_gaussian, -100, 100)
        # Get log-prob for squashed Gaussian.
        return torch.sum(
            log_prob_gaussian - self._log_squash_grad(unsquashed_values),
            dim=-1)

    def _squash(self, unsquashed_values):
        """Squash an array element-wise into the (high, low) range

        Arguments:
            unsquashed_values: values to be squashed

        Returns:
            The squashed values.  The output shape is `unsquashed_values.shape`

        """
        raise NotImplementedError

    def _unsquash(self, values):
        """Unsquash an array element-wise from the (high, low) range

        Arguments:
            squashed_values: values to be unsquashed

        Returns:
            The unsquashed values.  The output shape is `squashed_values.shape`

        """
        raise NotImplementedError

    def _log_squash_grad(self, unsquashed_values):
        """Log gradient of _squash with respect to its argument.

        Arguments:
            squashed_values:  Point at which to measure the gradient.

        Returns:
            The gradient at the given point.  The output shape is
            `squashed_values.shape`.

        """
        raise NotImplementedError


class TorchSquashedGaussian(_TorchSquashedGaussianBase):
    """A tanh-squashed Gaussian distribution defined by: mean, std, low, high.

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """

    def _log_squash_grad(self, unsquashed_values):
        unsquashed_values_tanhd = torch.tanh(unsquashed_values)
        return torch.log(1 - unsquashed_values_tanhd**2 + SMALL_NUMBER)

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        raise ValueError("Entropy not defined for SquashedGaussian!")

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        raise ValueError("KL not defined for SquashedGaussian!")

    def _squash(self, raw_values: TensorType) -> TensorType:
        # Returned values are within [low, high] (including `low` and `high`).
        squashed = ((torch.tanh(raw_values) + 1.0) / 2.0) * \
            (self.high - self.low) + self.low
        return torch.clamp(squashed, self.low, self.high)

    def _unsquash(self, values: TensorType) -> TensorType:
        normed_values = (values - self.low) / (self.high - self.low) * 2.0 - \
                        1.0
        # Stabilize input to atanh.
        save_normed_values = torch.clamp(normed_values, -1.0 + SMALL_NUMBER,
                                         1.0 - SMALL_NUMBER)
        unsquashed = atanh(save_normed_values)
        return unsquashed

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape) * 2


class TorchGaussianSquashedGaussian(_TorchSquashedGaussianBase):
    """A gaussian CDF-squashed Gaussian distribution.

    Can be used instead of the `SquashedGaussian` in case entropy or KL need
    to be computable in analytical form (`SquashedGaussian` can only provide
    those empirically).

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """
    # Chosen to match the standard logistic variance, so that:
    #   Var(N(0, 2 * _SCALE)) = Var(Logistic(0, 1))
    _SCALE = 0.5 * 1.8137
    SQUASH_DIST = \
        torch.distributions.normal.Normal(0.0, _SCALE) if torch else None

    @override(_TorchSquashedGaussianBase)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scale = torch.from_numpy(np.array(self._SCALE))
        if self.model:
            self.scale = self.scale.to(
                next(iter(self.model.parameters())).device)

    @override(ActionDistribution)
    def kl(self, other):
        # KL(self || other) is just the KL of the two unsquashed distributions.
        assert isinstance(other, TorchGaussianSquashedGaussian)

        mean = self.distr.loc
        std = self.distr.scale

        other_mean = other.distr.loc
        other_std = other.distr.scale

        return torch.sum(
            (other.log_std - self.log_std +
             (torch.pow(std, 2.0) + torch.pow(mean - other_mean, 2.0)) /
             (2.0 * torch.pow(other_std, 2.0)) - 0.5),
            axis=1)

    def entropy(self):
        # Entropy is:
        #   -KL(self.distr || N(0, _SCALE)) + log(high - low)
        # where the latter distribution's CDF is used to do the squashing.

        mean = self.distr.loc
        std = self.distr.scale

        return torch.sum(
            torch.log(self.high - self.low) -
            (torch.log(self.scale) - self.log_std +
             (torch.pow(std, 2.0) + torch.pow(mean, 2.0)) /
             (2.0 * torch.pow(self.scale, 2.0)) - 0.5),
            dim=1)

    def _log_squash_grad(self, unsquashed_values):
        log_grad = self.SQUASH_DIST.log_prob(value=unsquashed_values)
        log_grad += torch.log(self.high - self.low)
        return log_grad

    def _squash(self, raw_values):
        # Make sure raw_values are not too high/low (such that tanh would
        # return exactly 1.0/-1.0, which would lead to +/-inf log-probs).

        values = self.SQUASH_DIST.cdf(raw_values)  # / self._SCALE)
        return (torch.clamp(values, SMALL_NUMBER, 1.0 - SMALL_NUMBER) *
                (self.high - self.low) + self.low)

    def _unsquash(self, values):
        x = (values - self.low) / (self.high - self.low)
        return self.SQUASH_DIST.icdf(x)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape) * 2


class TorchBeta(TorchDistributionWrapper):
    """
    A Beta distribution is defined on the interval [0, 1] and parameterized by
    shape parameters alpha and beta (also called concentration parameters).

    PDF(x; alpha, beta) = x**(alpha - 1) (1 - x)**(beta - 1) / Z
        with Z = Gamma(alpha) Gamma(beta) / Gamma(alpha + beta)
        and Gamma(n) = (n - 1)!
    """

    def __init__(self,
                 inputs: List[TensorType],
                 model: TorchModelV2,
                 low: float = 0.0,
                 high: float = 1.0):
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
    def deterministic_sample(self) -> TensorType:
        self.last_sample = self._squash(self.dist.mean)
        return self.last_sample

    @override(TorchDistributionWrapper)
    def sample(self) -> TensorType:
        # Use the reparameterization version of `dist.sample` to allow for
        # the results to be backprop'able e.g. in a loss term.
        normal_sample = self.dist.rsample()
        self.last_sample = self._squash(normal_sample)
        return self.last_sample

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        unsquashed_values = self._unsquash(x)
        return torch.sum(self.dist.log_prob(unsquashed_values), dim=-1)

    def _squash(self, raw_values: TensorType) -> TensorType:
        return raw_values * (self.high - self.low) + self.low

    def _unsquash(self, values: TensorType) -> TensorType:
        return (values - self.low) / (self.high - self.low)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape) * 2


class TorchDeterministic(TorchDistributionWrapper):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).
    """

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        return self.inputs

    @override(TorchDistributionWrapper)
    def sampled_action_logp(self) -> TensorType:
        return torch.zeros((self.inputs.size()[0], ), dtype=torch.float32)

    @override(TorchDistributionWrapper)
    def sample(self) -> TensorType:
        return self.deterministic_sample()

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
            action_space: gym.Space,
            model_config: ModelConfigDict) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape)


class TorchMultiActionDistribution(TorchDistributionWrapper):
    """Action distribution that operates on multiple, possibly nested actions.
    """

    def __init__(self, inputs, model, *, child_distributions, input_lens,
                 action_space):
        """Initializes a TorchMultiActionDistribution object.

        Args:
            inputs (torch.Tensor): A single tensor of shape [BATCH, size].
            model (TorchModelV2): The TorchModelV2 object used to produce
                inputs for this distribution.
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
            inputs = torch.from_numpy(inputs)
            if isinstance(model, TorchModelV2):
                inputs = inputs.to(next(model.parameters()).device)
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


class TorchDirichlet(TorchDistributionWrapper):
    """Dirichlet distribution for continuous actions that are between
    [0,1] and sum to 1.

    e.g. actions that represent resource allocation."""

    def __init__(self, inputs, model):
        """Input is a tensor of logits. The exponential of logits is used to
        parametrize the Dirichlet distribution as all parameters need to be
        positive. An arbitrary small epsilon is added to the concentration
        parameters to be zero due to numerical error.

        See issue #4440 for more details.
        """
        self.epsilon = torch.tensor(1e-7).to(inputs.device)
        concentration = torch.exp(inputs) + self.epsilon
        self.dist = torch.distributions.dirichlet.Dirichlet(
            concentration=concentration,
            validate_args=True,
        )
        super().__init__(concentration, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        self.last_sample = nn.functional.softmax(self.dist.concentration)
        return self.last_sample

    @override(ActionDistribution)
    def logp(self, x):
        # Support of Dirichlet are positive real numbers. x is already
        # an array of positive numbers, but we clip to avoid zeros due to
        # numerical errors.
        x = torch.max(x, self.epsilon)
        x = x / torch.sum(x, dim=-1, keepdim=True)
        return self.dist.log_prob(x)

    @override(ActionDistribution)
    def entropy(self):
        return self.dist.entropy()

    @override(ActionDistribution)
    def kl(self, other):
        return self.dist.kl_divergence(other.dist)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape)
