import copy
import logging
import pickle
try:  # Python 3 only -- needed for lint test.
    import bayes_opt as byo
except ImportError:
    byo = None

from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class BayesOptSearch(Searcher):
    """Uses fmfn/BayesianOptimization to optimize hyperparameters.

    fmfn/BayesianOptimization is a library for Bayesian Optimization. More
    info can be found here: https://github.com/fmfn/BayesianOptimization.

    You will need to install fmfn/BayesianOptimization via the following:

    .. code-block:: bash

        pip install bayesian-optimization

    This algorithm requires setting a search space using the
    `BayesianOptimization search space specification`_.

    Parameters:
        space (dict): Continuous search space. Parameters will be sampled from
            this space which will be used to run trials.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        utility_kwargs (dict): Parameters to define the utility function.
            The default value is a dictionary with three keys:
            - kind: ucb (Upper Confidence Bound)
            - kappa: 2.576
            - xi: 0.0
        random_state (int): Used to initialize BayesOpt.
        analysis (ExperimentAnalysis): Optionally, the previous analysis
            to integrate.
        verbose (int): Sets verbosity level for BayesOpt packages.
        max_concurrent: Deprecated.
        use_early_stopped_trials: Deprecated.

    .. code-block:: python

        from ray import tune
        from ray.tune.suggest.bayesopt import BayesOptSearch

        space = {
            'width': (0, 20),
            'height': (-100, 100),
        }
        algo = BayesOptSearch(space, metric="mean_loss", mode="min")
        tune.run(my_func, algo=algo)
    """
    # bayes_opt.BayesianOptimization: Optimization object
    optimizer = None

    def __init__(self,
                 space,
                 metric="episode_reward_mean",
                 mode="max",
                 utility_kwargs=None,
                 random_state=1,
                 n_initial_trials=100,
                 verbose=0,
                 analysis=None,
                 max_concurrent=None,
                 use_early_stopped_trials=None):
        """Instantiate new BayesOptSearch object.

        Parameters:
            space (dict): Continuous search space.
                Parameters will be sampled from
                this space which will be used to run trials.
            metric (str): The training result objective value attribute.
            mode (str): One of {min, max}. Determines whether objective is
                minimizing or maximizing the metric attribute.
            utility_kwargs (dict): Parameters to define the utility function.
                Must provide values for the keys `kind`, `kappa`, and `xi`.
            random_state (int): Used to initialize BayesOpt.
            analysis (ExperimentAnalysis): Optionally, the previous analysis
                to integrate.
            verbose (int): Sets verbosity level for BayesOpt packages.
            max_concurrent: Deprecated.
            use_early_stopped_trials: Deprecated.
        """
        assert byo is not None, (
            "BayesOpt must be installed!. You can install BayesOpt with"
            " the command: `pip install bayesian-optimization`.")
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        self.max_concurrent = max_concurrent
        super(BayesOptSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)

        if utility_kwargs is None:
            # The defaults arguments are the same
            # as in the package BayesianOptimization
            utility_kwargs = dict(
                kind="ucb",
                kappa=2.576,
                xi=0.0,
            )

        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._live_trial_mapping = {}
        self._cached_results = []
        self.n_initial_trials = n_initial_trials

        self.optimizer = byo.BayesianOptimization(
            f=None, pbounds=space, verbose=verbose, random_state=random_state)

        self.utility = byo.UtilityFunction(**utility_kwargs)
        if analysis is not None:
            self.register_analysis(analysis)

    def suggest(self, trial_id):
        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None
        new_trial = self.optimizer.suggest(self.utility)

        self._live_trial_mapping[trial_id] = new_trial

        return copy.deepcopy(new_trial)

    def register_analysis(self, analysis):
        """Integrate the given analysis into the gaussian process.

        Parameters
        ------------------
        analysis (ExperimentAnalysis): Optionally, the previous analysis
            to integrate.
        """
        for (_, report), params in zip(analysis.dataframe().iterrows(),
                                       analysis.get_all_configs().values()):
            # We add the obtained results to the
            # gaussian process optimizer
            self.optimizer.register(
                params=params, target=self._metric_op * report[self._metric])

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial."""
        if result:
            if self.n_initial_trials:
                self._cached_results.append(self._live_trial_mapping[trial_id], result)
                if len(self._cached_results) < self.n_initial_trials:
                    return
                else:
                    self.n_initial_trials = 0
                    for param, result in self._cached_results:
                        self._process_result(param, result)
            else:
                self._process_result(self._live_trial_mapping[trial_id], result)
        del self._live_trial_mapping[trial_id]

    def _process_result(self, params, result):
        self.optimizer.register(
            params=params,
            target=self._metric_op * result[self.metric])

    def save(self, checkpoint_dir):
        trials_object = self.optimizer
        with open(checkpoint_dir, "wb") as output:
            pickle.dump(trials_object, output)

    def restore(self, checkpoint_dir):
        with open(checkpoint_dir, "rb") as input:
            trials_object = pickle.load(input)
        self.optimizer = trials_object
