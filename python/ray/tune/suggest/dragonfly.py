from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import inspect
import logging
import pickle
from typing import Dict, List, Optional, Union

from ray.tune.sample import Domain, Float, Quantized
from ray.tune.suggest.suggestion import UNRESOLVED_SEARCH_SPACE
from ray.tune.suggest.variant_generator import parse_spec_vars
from ray.tune.utils.util import flatten_dict

try:  # Python 3 only -- needed for lint test.
    import dragonfly
    from dragonfly.opt.blackbox_optimiser import BlackboxOptimiser
except ImportError:
    dragonfly = None
    BlackboxOptimiser = None

from ray.tune.suggest.suggestion import Searcher

logger = logging.getLogger(__name__)


class DragonflySearch(Searcher):
    """Uses Dragonfly to optimize hyperparameters.

    Dragonfly provides an array of tools to scale up Bayesian optimisation to
    expensive large scale problems, including high dimensional optimisation.
    parallel evaluations in synchronous or asynchronous settings,
    multi-fidelity optimisation (using cheap approximations to speed up the
    optimisation process), and multi-objective optimisation. For more info:

    * Dragonfly Website: https://github.com/dragonfly/dragonfly
    * Dragonfly Documentation: https://dragonfly-opt.readthedocs.io/

    To use this search algorithm, install Dragonfly:

    .. code-block:: bash

        $ pip install dragonfly-opt


    This interface requires using FunctionCallers and optimizers provided by
    Dragonfly.

    Parameters:
        optimizer (dragonfly.opt.BlackboxOptimiser|str): Optimizer provided
            from dragonfly. Choose an optimiser that extends BlackboxOptimiser.
            If this is a string, `domain` must be set and `optimizer` must be
            one of [random, bandit, genetic].
        domain (str): Optional domain. Should only be set if you don't pass
            an optimizer as the `optimizer` argument.
            Has to be one of [cartesian, euclidean].
        space (list|dict): Search space. Should only be set if you don't pass
            an optimizer as the `optimizer` argument. Defines the search space
            and requires a `domain` to be set. Can be automatically converted
            from the `config` dict passed to `tune.run()`.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate (list of lists): A list of points you'd like to run
            first before sampling from the optimiser, e.g. these could be
            parameter configurations you already know work well to help
            the optimiser select good values. Each point is a list of the
            parameters using the order definition given by parameter_names.
        evaluated_rewards (list): If you have previously evaluated the
            parameters passed in as points_to_evaluate you can avoid
            re-running those trials by passing in the reward attributes
            as a list so the optimiser can be told the results without
            needing to re-compute the trial. Must be the same length as
            points_to_evaluate.

    Tune automatically converts search spaces to Dragonfly's format:


    .. code-block:: python

        from ray import tune

        config = {
            "LiNO3_vol": tune.uniform(0, 7),
            "Li2SO4_vol": tune.uniform(0, 7),
            "NaClO4_vol": tune.uniform(0, 7)
        }

        df_search = DragonflySearch(
            optimizer="bandit",
            domain="euclidean",
            metric="objective",
            mode="max")

        tune.run(my_func, config=config, search_alg=df_search)

    If you would like to pass the search space/optimizer manually,
    the code would look like this:

    .. code-block:: python

        from ray import tune

        space = [{
            "name": "LiNO3_vol",
            "type": "float",
            "min": 0,
            "max": 7
        }, {
            "name": "Li2SO4_vol",
            "type": "float",
            "min": 0,
            "max": 7
        }, {
            "name": "NaClO4_vol",
            "type": "float",
            "min": 0,
            "max": 7
        }]

        df_search = DragonflySearch(
            optimizer="bandit",
            domain="euclidean",
            space=space,
            metric="objective",
            mode="max")

        tune.run(my_func, search_alg=df_search)

    """

    def __init__(self,
                 optimizer: Optional[BlackboxOptimiser] = None,
                 domain: Optional[str] = None,
                 space: Optional[Union[Dict, List[Dict]]] = None,
                 metric: Optional[str] = None,
                 mode: Optional[str] = None,
                 points_to_evaluate: Optional[List[List]] = None,
                 evaluated_rewards: Optional[List] = None,
                 **kwargs):
        assert dragonfly is not None, """dragonfly must be installed!
            You can install Dragonfly with the command:
            `pip install dragonfly-opt`."""
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."

        super(DragonflySearch, self).__init__(
            metric=metric, mode=mode, **kwargs)

        self._opt_arg = optimizer
        self._domain = domain

        if isinstance(space, dict) and space:
            resolved_vars, domain_vars, grid_vars = parse_spec_vars(space)
            if domain_vars or grid_vars:
                logger.warning(
                    UNRESOLVED_SEARCH_SPACE.format(
                        par="space", cls=type(self)))
                space = self.convert_search_space(space)

        self._space = space
        self._points_to_evaluate = points_to_evaluate
        self._evaluated_rewards = evaluated_rewards
        self._initial_points = []
        self._live_trial_mapping = {}

        self._opt = None
        if isinstance(optimizer, BlackboxOptimiser):
            if domain or space:
                raise ValueError(
                    "If you pass an optimizer instance to dragonfly, do not "
                    "pass a `domain` or `space`.")
            self._opt = optimizer
            self.init_dragonfly()
        elif self._space:
            self.setup_dragonfly()

    def setup_dragonfly(self):
        """Setup dragonfly when no optimizer has been passed."""
        assert not self._opt, "Optimizer already set."

        from dragonfly import load_config
        from dragonfly.exd.experiment_caller import CPFunctionCaller, \
            EuclideanFunctionCaller
        from dragonfly.opt.blackbox_optimiser import BlackboxOptimiser
        from dragonfly.opt.random_optimiser import CPRandomOptimiser, \
            EuclideanRandomOptimiser
        from dragonfly.opt.cp_ga_optimiser import CPGAOptimiser
        from dragonfly.opt.gp_bandit import CPGPBandit, EuclideanGPBandit

        if not self._space:
            raise ValueError(
                "You have to pass a `space` when initializing dragonfly, or "
                "pass a search space definition to the `config` parameter "
                "of `tune.run()`.")

        if not self._domain:
            raise ValueError(
                "You have to set a `domain` when initializing dragonfly. "
                "Choose one of [Cartesian, Euclidean].")

        if self._domain.lower().startswith("cartesian"):
            function_caller_cls = CPFunctionCaller
        elif self._domain.lower().startswith("euclidean"):
            function_caller_cls = EuclideanFunctionCaller
        else:
            raise ValueError("Dragonfly's `domain` argument must be one of "
                             "[Cartesian, Euclidean].")

        optimizer_cls = None
        if inspect.isclass(self._opt_arg) and issubclass(
                self._opt_arg, BlackboxOptimiser):
            optimizer_cls = self._opt_arg
        elif isinstance(self._opt_arg, str):
            if self._opt_arg.lower().startswith("random"):
                if function_caller_cls == CPFunctionCaller:
                    optimizer_cls = CPRandomOptimiser
                else:
                    optimizer_cls = EuclideanRandomOptimiser
            elif self._opt_arg.lower().startswith("bandit"):
                if function_caller_cls == CPFunctionCaller:
                    optimizer_cls = CPGPBandit
                else:
                    optimizer_cls = EuclideanGPBandit
            elif self._opt_arg.lower().startswith("genetic"):
                if function_caller_cls == CPFunctionCaller:
                    optimizer_cls = CPGAOptimiser
                else:
                    raise ValueError(
                        "Currently only the `cartesian` domain works with "
                        "the `genetic` optimizer.")
            else:
                raise ValueError(
                    "Invalid optimizer specification. Either pass a full "
                    "dragonfly optimizer, or a string "
                    "in [random, bandit, genetic].")

        assert optimizer_cls, "No optimizer could be determined."
        domain_config = load_config({"domain": self._space})
        function_caller = function_caller_cls(
            None, domain_config.domain.list_of_domains[0])
        self._opt = optimizer_cls(function_caller, ask_tell_mode=True)
        self.init_dragonfly()

    def init_dragonfly(self):
        self._opt.initialise()
        if self._points_to_evaluate and self._evaluated_rewards:
            self._opt.tell([(self._points_to_evaluate,
                             self._evaluated_rewards)])
        elif self._points_to_evaluate:
            self._initial_points = self._points_to_evaluate
        # Dragonfly internally maximizes, so "min" => -1
        if self._mode == "min":
            self._metric_op = -1.
        elif self._mode == "max":
            self._metric_op = 1.

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

        self.setup_dragonfly()
        return True

    def suggest(self, trial_id: str) -> Optional[Dict]:
        if not self._opt:
            raise RuntimeError(
                "Trying to sample a configuration from {}, but no search "
                "space has been defined. Either pass the `{}` argument when "
                "instantiating the search algorithm, or pass a `config` to "
                "`tune.run()`.".format(self.__class__.__name__, "space"))

        if self._initial_points:
            suggested_config = self._initial_points[0]
            del self._initial_points[0]
        else:
            try:
                suggested_config = self._opt.ask()
            except Exception as exc:
                logger.warning(
                    "Dragonfly errored when querying. This may be due to a "
                    "higher level of parallelism than supported. Try reducing "
                    "parallelism in the experiment: %s", str(exc))
                return None
        self._live_trial_mapping[trial_id] = suggested_config
        return {"point": suggested_config}

    def on_trial_complete(self,
                          trial_id: str,
                          result: Optional[Dict] = None,
                          error: bool = False):
        """Passes result to Dragonfly unless early terminated or errored."""
        trial_info = self._live_trial_mapping.pop(trial_id)
        if result:
            self._opt.tell([(trial_info,
                             self._metric_op * result[self._metric])])

    def save(self, checkpoint_path: str):
        trials_object = (self._initial_points, self._opt)
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(trials_object, outputFile)

    def restore(self, checkpoint_dir: str):
        with open(checkpoint_dir, "rb") as inputFile:
            trials_object = pickle.load(inputFile)
        self._initial_points = trials_object[0]
        self._opt = trials_object[1]

    @staticmethod
    def convert_search_space(spec: Dict) -> List[Dict]:
        spec = flatten_dict(spec, prevent_delimiter=True)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a Dragonfly search space.")

        def resolve_value(par: str, domain: Domain) -> Dict:
            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                logger.warning(
                    "Dragonfly search does not support quantization. "
                    "Dropped quantization.")
                sampler = sampler.get_sampler()

            if isinstance(domain, Float):
                if domain.sampler is not None:
                    logger.warning(
                        "Dragonfly does not support specific sampling methods."
                        " The {} sampler will be dropped.".format(sampler))
                return {
                    "name": par,
                    "type": "float",
                    "min": domain.lower,
                    "max": domain.upper
                }

            raise ValueError("Dragonfly does not support parameters of type "
                             "`{}`".format(type(domain).__name__))

        # Parameter name is e.g. "a/b/c" for nested dicts
        space = [
            resolve_value("/".join(path), domain)
            for path, domain in domain_vars
        ]

        return space
