import logging
import pickle
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from ray.tune.result import DEFAULT_METRIC
from ray.tune.sample import Categorical, Domain, Float, Integer, LogUniform, \
    Quantized, Uniform
from ray.tune.suggest.suggestion import UNRESOLVED_SEARCH_SPACE, \
    UNDEFINED_METRIC_MODE, UNDEFINED_SEARCH_SPACE
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils.util import is_nan_or_inf, unflatten_dict, \
    validate_warmstart

try:  # Python 3 only -- needed for lint test.
    import hebo
    import torch  # hebo has torch as a dependency
except ImportError:
    hebo = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)

SPACE_ERROR_MESSAGE = ("Space must be either a HEBO DesignSpace object"
                       "or a dictionary with ONLY tune search spaces.")


class HEBOSearch(Searcher):
    """Uses HEBO (Heteroscedastic Evolutionary Bayesian Optimization)
    to optimize hyperparameters.

    HEBO is a cutting edge black-box optimization framework created
    by Huawei's Noah Ark. More info can be found here:
    https://github.com/huawei-noah/noah-research/tree/master/HEBO.

    You will need to install HEBO via the following:

    .. code-block:: bash

        pip install HEBO

    `space` can either be a HEBO's `DesignSpace` object or a dict of Tune
    search spaces.

    Please note that the first few trials will be random and used
    to kickstart the search process. In order to achieve good results,
    we recommend setting the number of trials to at least 15.

    Args:
        space (dict|hebo.design_space.design_space.DesignSpace):
            A dict mapping parameter names to Tune search spaces or a
            HEBO DesignSpace object.
        metric (str): The training result objective value attribute. If None
            but a mode was passed, the anonymous metric `_metric` will be used
            per default.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate (list): Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want to run first to help the algorithm make better suggestions
            for future parameters. Needs to be a list of dicts containing the
            configurations.
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate. (See tune/examples/hebo_example.py)
        random_state_seed (int, None): Seed for reproducible
            results. Defaults to None. Please note that setting this to a value
            will change global random states for `numpy` and `torch`
            on initalization and loading from checkpoint.
        n_suggestions (int, 1): If higher than one, suggestions will
            be made in batches and cached. Higher values may increase
            convergence speed in certain cases (authors recommend 8).
            For best results, wrap this searcher in a
            ``ConcurrencyLimiter(max_concurrent=n, batch=True)``
            where `n == n_suggestions`.
            Refer to `tune/examples/hebo_example.py`.
        **kwargs: The keyword arguments will be passed to `HEBO()``.

    Tune automatically converts search spaces to HEBO's format:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.hebo import HEBOSearch

        config = {
            "width": tune.uniform(0, 20),
            "height": tune.uniform(-100, 100)
        }

        hebo = HEBOSearch(metric="mean_loss", mode="min")
        tune.run(my_func, config=config, search_alg=hebo)

    Alternatively, you can pass a HEBO `DesignSpace` object manually to the
    Searcher:

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.hebo import HEBOSearch
        from hebo.design_space.design_space import DesignSpace

        space_config = [
            {'name' : 'width', 'type' : 'num', 'lb' : 0, 'ub' : 20},
            {'name' : 'height', 'type' : 'num', 'lb' : -100, 'ub' : 100},
        ]
        space = DesignSpace().parse(space_config)

        hebo = HEBOSearch(space, metric="mean_loss", mode="min")
        tune.run(my_func, search_alg=hebo)

    """

    def __init__(
            self,
            space: Optional[Union[
                Dict, "hebo.design_space.design_space.DesignSpace"]] = None,
            metric: Optional[str] = None,
            mode: Optional[str] = None,
            points_to_evaluate: Optional[List[Dict]] = None,
            evaluated_rewards: Optional[List] = None,
            random_state_seed: Optional[int] = None,
            n_suggestions: int = 1,
            **kwargs):
        assert hebo is not None, (
            "HEBO must be installed!. You can install HEBO with"
            " the command: `pip install HEBO`.")
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
        assert isinstance(n_suggestions, int) and n_suggestions >= 1, (
            "`n_suggestions` must be an integer and at least 1.")
        if random_state_seed is not None:
            assert isinstance(
                random_state_seed, int
            ), "random_state_seed must be None or int, got '{}'.".format(
                type(random_state_seed))
        super(HEBOSearch, self).__init__(metric=metric, mode=mode)

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if resolved_vars:
                raise TypeError(SPACE_ERROR_MESSAGE)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(
                        par="space", cls=type(self)))
                space = self.convert_search_space(space)
        elif space is not None and not isinstance(
                space, hebo.design_space.design_space.DesignSpace):
            raise TypeError(SPACE_ERROR_MESSAGE +
                            " Got {}.".format(type(space)))

        self._hebo_config = kwargs
        self._random_state_seed = random_state_seed
        self._space = space
        self._points_to_evaluate = points_to_evaluate
        self._evaluated_rewards = evaluated_rewards
        self._initial_points = []
        self._live_trial_mapping = {}

        self._n_suggestions = n_suggestions
        self._suggestions_cache = []

        self._opt = None
        if space:
            self._setup_optimizer()

    def _setup_optimizer(self):
        # HEBO internally minimizes, so "max" => -1
        if self._mode == "max":
            self._metric_op = -1.
        elif self._mode == "min":
            self._metric_op = 1.

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        if not isinstance(self._space,
                          hebo.design_space.design_space.DesignSpace):
            raise ValueError(
                f"Invalid search space: {type(self._space)}. Either pass a "
                f"valid search space to the `HEBOSearch` class or pass "
                f"a `config` parameter to `tune.run()`")

        if self._space.num_paras <= 0:
            raise ValueError(
                "Got empty search space. Please make sure to pass "
                "a valid search space with at least one parameter to "
                "`HEBOSearch`")

        if self._random_state_seed is not None:
            np.random.seed(self._random_state_seed)
            torch.random.manual_seed(self._random_state_seed)

        self._opt = hebo.optimizers.hebo.HEBO(
            space=self._space, **self._hebo_config)

        if self._points_to_evaluate:
            validate_warmstart(self._space.para_names,
                               self._points_to_evaluate,
                               self._evaluated_rewards)
            if self._evaluated_rewards:
                self._opt.observe(
                    pd.DataFrame(self._points_to_evaluate),
                    np.array(self._evaluated_rewards) * self._metric_op)
            else:
                self._initial_points = self._points_to_evaluate

    def set_search_properties(self, metric: Optional[str], mode: Optional[str],
                              config: Dict) -> bool:
        if self._opt:
            return False
        space = self.convert_search_space(config)
        self._space = space
        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._setup_optimizer()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._opt:
            raise RuntimeError(
                UNDEFINED_SEARCH_SPACE.format(
                    cls=self.__class__.__name__, space="space"))

        if not self._metric or not self._mode:
            raise RuntimeError(
                UNDEFINED_METRIC_MODE.format(
                    cls=self.__class__.__name__,
                    metric=self._metric,
                    mode=self._mode))

        if self._initial_points:
            params = self._initial_points.pop(0)
            suggestion = pd.DataFrame(params, index=[0])
        else:
            if not self._suggestions_cache:
                suggestion = self._opt.suggest(
                    n_suggestions=self._n_suggestions)
                self._suggestions_cache = suggestion.to_dict("records")
            params = self._suggestions_cache.pop(0)
            suggestion = pd.DataFrame(params, index=[0])
        self._live_trial_mapping[trial_id] = suggestion
        return unflatten_dict(params)

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
        """Notification for the completion of trial.

        HEBO always minimizes."""

        if result:
            self._process_result(trial_id, result)
        self._live_trial_mapping.pop(trial_id)

    def _process_result(self, trial_id: str, result: Dict):
        trial_info = self._live_trial_mapping[trial_id]
        if result and not is_nan_or_inf(result[self._metric]):
            self._opt.observe(
                trial_info, np.array([self._metric_op * result[self._metric]]))

    def save(self, checkpoint_path: str):
        """Storing current optimizer state."""
        if self._random_state_seed is not None:
            numpy_random_state = np.random.get_state()
            torch_random_state = torch.get_rng_state()
        else:
            numpy_random_state = None
            torch_random_state = None
        with open(checkpoint_path, "wb") as f:
            pickle.dump((self._opt, self._points_to_evaluate,
                         numpy_random_state, torch_random_state), f)

    def restore(self, checkpoint_path: str):
        """Restoring current optimizer state."""
        with open(checkpoint_path, "rb") as f:
            (self._opt, self._points_to_evaluate, numpy_random_state,
             torch_random_state) = pickle.load(f)
        if numpy_random_state is not None:
            np.random.set_state(numpy_random_state)
        if torch_random_state is not None:
            torch.random.set_rng_state(torch_random_state)

    @staticmethod
    def convert_search_space(spec: Dict, prefix: str = "") -> Dict:
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        params = []

        if not domain_vars and not grid_vars:
            return {}

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a HEBO search space.")

        def resolve_value(par: str, domain: Domain):
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning("HEBO search does not support quantization. "
                               "Dropped quantization.")
                sampler = sampler.get_sampler()

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    return {
                        "name": par,
                        "type": "pow",
                        "lb": domain.lower,
                        "ub": domain.upper,
                        "base": sampler.base
                    }
                elif isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "num",
                        "lb": domain.lower,
                        "ub": domain.upper
                    }

            elif isinstance(domain, Integer):
                if isinstance(sampler, LogUniform):
                    return {
                        "name": par,
                        "type": "pow_int",
                        "lb": domain.lower,
                        "ub": domain.upper,
                        "base": sampler.base
                    }
                elif isinstance(sampler, Uniform):
                    return {
                        "name": par,
                        "type": "int",
                        "lb": domain.lower,
                        "ub": domain.upper
                    }
            elif isinstance(domain, Categorical):
                return {
                    "name": par,
                    "type": "cat",
                    "categories": list(domain.categories)
                }

            raise ValueError("HEBO does not support parameters of type "
                             "`{}` with samplers of type `{}`".format(
                                 type(domain).__name__,
                                 type(domain.sampler).__name__))

        for path, domain in domain_vars:
            par = "/".join(
                [str(p) for p in ((prefix, ) + path if prefix else path)])
            value = resolve_value(par, domain)
            params.append(value)

        return hebo.design_space.design_space.DesignSpace().parse(params)
