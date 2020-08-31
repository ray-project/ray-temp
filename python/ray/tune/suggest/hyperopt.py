from typing import Dict

import numpy as np
import copy
import logging
from functools import partial
import pickle

from ray.tune.sample import Categorical, Float, Integer, LogUniform, Normal, \
    Quantized, \
    Uniform
from ray.tune.suggest.variant_generator import assign_value, parse_spec_vars

try:
    hyperopt_logger = logging.getLogger("hyperopt")
    hyperopt_logger.setLevel(logging.WARNING)
    import hyperopt as hpo
except ImportError:
    hpo = None

from ray.tune.error import TuneError
from ray.tune.suggest import Searcher

logger = logging.getLogger(__name__)


class HyperOptSearch(Searcher):
    """A wrapper around HyperOpt to provide trial suggestions.

    HyperOpt a Python library for serial and parallel optimization
    over awkward search spaces, which may include real-valued, discrete,
    and conditional dimensions. More info can be found at
    http://hyperopt.github.io/hyperopt.

    HyperOptSearch uses the Tree-structured Parzen Estimators algorithm,
    though it can be trivially extended to support any algorithm HyperOpt
    supports.

    To use this search algorithm, you will need to install HyperOpt:

    .. code-block:: bash

        pip install -U hyperopt

    You will not be able to leverage Tune's default ``grid_search``
    and random search primitives when using HyperOptSearch. You need to
    use the `HyperOpt search space specification
    <https://github.com/hyperopt/hyperopt/wiki/FMin>`_.

    .. code-block:: python

        space = {
            'width': hp.uniform('width', 0, 20),
            'height': hp.uniform('height', -100, 100),
            'activation': hp.choice("activation", ["relu", "tanh"])
        }
        current_best_params = [{
            'width': 10,
            'height': 0,
            'activation': 0, # The index of "relu"
        }]
        algo = HyperOptSearch(
            space, metric="mean_loss", mode="min",
            points_to_evaluate=current_best_params)


    Parameters:
        space (dict): HyperOpt configuration. Parameters will be sampled
            from this configuration and will be used to override
            parameters generated in the variant generation process.
        metric (str): The training result objective value attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        points_to_evaluate (list): Initial parameter suggestions to be run
            first. This is for when you already have some good parameters
            you want hyperopt to run first to help the TPE algorithm
            make better suggestions for future parameters. Needs to be
            a list of dict of hyperopt-named variables.
            Choice variables should be indicated by their index in the
            list (see example)
        n_initial_points (int): number of random evaluations of the
            objective function before starting to aproximate it with
            tree parzen estimators. Defaults to 20.
        random_state_seed (int, array_like, None): seed for reproducible
            results. Defaults to None.
        gamma (float in range (0,1)): parameter governing the tree parzen
            estimators suggestion algorithm. Defaults to 0.25.
        max_concurrent: Deprecated.
        use_early_stopped_trials: Deprecated.

    """

    def __init__(
            self,
            space,
            metric="episode_reward_mean",
            mode="max",
            points_to_evaluate=None,
            n_initial_points=20,
            random_state_seed=None,
            gamma=0.25,
            max_concurrent=None,
            use_early_stopped_trials=None,
    ):
        assert hpo is not None, (
            "HyperOpt must be installed! Run `pip install hyperopt`.")
        from hyperopt.fmin import generate_trials_to_calculate
        super(HyperOptSearch, self).__init__(
            metric=metric,
            mode=mode,
            max_concurrent=max_concurrent,
            use_early_stopped_trials=use_early_stopped_trials)
        self.max_concurrent = max_concurrent
        # hyperopt internally minimizes, so "max" => -1
        if mode == "max":
            self.metric_op = -1.
        elif mode == "min":
            self.metric_op = 1.

        if n_initial_points is None:
            self.algo = hpo.tpe.suggest
        else:
            self.algo = partial(
                hpo.tpe.suggest, n_startup_jobs=n_initial_points)
        if gamma is not None:
            self.algo = partial(self.algo, gamma=gamma)
        self.domain = hpo.Domain(lambda spc: spc, space)
        if points_to_evaluate is None:
            self._hpopt_trials = hpo.Trials()
            self._points_to_evaluate = 0
        else:
            assert type(points_to_evaluate) == list
            self._hpopt_trials = generate_trials_to_calculate(
                points_to_evaluate)
            self._hpopt_trials.refresh()
            self._points_to_evaluate = len(points_to_evaluate)
        self._live_trial_mapping = {}
        if random_state_seed is None:
            self.rstate = np.random.RandomState()
        else:
            self.rstate = np.random.RandomState(random_state_seed)

    def suggest(self, trial_id):
        if self.max_concurrent:
            if len(self._live_trial_mapping) >= self.max_concurrent:
                return None
        if self._points_to_evaluate > 0:
            new_trial = self._hpopt_trials.trials[self._points_to_evaluate - 1]
            self._points_to_evaluate -= 1
        else:
            new_ids = self._hpopt_trials.new_trial_ids(1)
            self._hpopt_trials.refresh()

            # Get new suggestion from Hyperopt
            new_trials = self.algo(new_ids, self.domain, self._hpopt_trials,
                                   self.rstate.randint(2**31 - 1))
            self._hpopt_trials.insert_trial_docs(new_trials)
            self._hpopt_trials.refresh()
            new_trial = new_trials[0]
        self._live_trial_mapping[trial_id] = (new_trial["tid"], new_trial)

        # Taken from HyperOpt.base.evaluate
        config = hpo.base.spec_from_misc(new_trial["misc"])
        ctrl = hpo.base.Ctrl(self._hpopt_trials, current_trial=new_trial)
        memo = self.domain.memo_from_config(config)
        hpo.utils.use_obj_for_literal_in_memo(self.domain.expr, ctrl,
                                              hpo.base.Ctrl, memo)

        suggested_config = hpo.pyll.rec_eval(
            self.domain.expr,
            memo=memo,
            print_node_on_error=self.domain.rec_eval_print_node_on_error)
        return copy.deepcopy(suggested_config)

    def on_trial_result(self, trial_id, result):
        ho_trial = self._get_hyperopt_trial(trial_id)
        if ho_trial is None:
            return
        now = hpo.utils.coarse_utcnow()
        ho_trial["book_time"] = now
        ho_trial["refresh_time"] = now

    def on_trial_complete(self, trial_id, result=None, error=False):
        """Notification for the completion of trial.

        The result is internally negated when interacting with HyperOpt
        so that HyperOpt can "maximize" this value, as it minimizes on default.
        """
        ho_trial = self._get_hyperopt_trial(trial_id)
        if ho_trial is None:
            return
        ho_trial["refresh_time"] = hpo.utils.coarse_utcnow()
        if error:
            ho_trial["state"] = hpo.base.JOB_STATE_ERROR
            ho_trial["misc"]["error"] = (str(TuneError), "Tune Error")
            self._hpopt_trials.refresh()
        elif result:
            self._process_result(trial_id, result)
        del self._live_trial_mapping[trial_id]

    def _process_result(self, trial_id, result):
        ho_trial = self._get_hyperopt_trial(trial_id)
        if not ho_trial:
            return
        ho_trial["refresh_time"] = hpo.utils.coarse_utcnow()

        ho_trial["state"] = hpo.base.JOB_STATE_DONE
        hp_result = self._to_hyperopt_result(result)
        ho_trial["result"] = hp_result
        self._hpopt_trials.refresh()

    def _to_hyperopt_result(self, result):
        return {"loss": self.metric_op * result[self.metric], "status": "ok"}

    def _get_hyperopt_trial(self, trial_id):
        if trial_id not in self._live_trial_mapping:
            return
        hyperopt_tid = self._live_trial_mapping[trial_id][0]
        return [
            t for t in self._hpopt_trials.trials if t["tid"] == hyperopt_tid
        ][0]

    def get_state(self):
        return {
            "hyperopt_trials": self._hpopt_trials,
            "rstate": self.rstate.get_state()
        }

    def set_state(self, state):
        self._hpopt_trials = state["hyperopt_trials"]
        self.rstate.set_state(state["rstate"])

    def save(self, checkpoint_path):
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(self.get_state(), outputFile)

    def restore(self, checkpoint_path):
        with open(checkpoint_path, "rb") as inputFile:
            trials_object = pickle.load(inputFile)

        if isinstance(trials_object, tuple):
            self._hpopt_trials = trials_object[0]
            self.rstate.set_state(trials_object[1])
        else:
            self.set_state(trials_object)

    @classmethod
    def from_config(
            cls,
            config,
            metric="episode_reward_mean",
            mode="max",
            points_to_evaluate=None,
            n_initial_points=20,
            random_state_seed=None,
            gamma=0.25,
            max_concurrent=None,
            use_early_stopped_trials=None,
    ):
        space = cls.convert_search_space(config)
        return cls(space, metric, mode, points_to_evaluate, n_initial_points,
                   random_state_seed, gamma, max_concurrent,
                   use_early_stopped_trials)

    @staticmethod
    def convert_search_space(spec: Dict):
        spec = copy.deepcopy(spec)
        resolved_vars, domain_vars, grid_vars = parse_spec_vars(spec)

        if not domain_vars and not grid_vars:
            return []

        if grid_vars:
            raise ValueError(
                "Grid search parameters cannot be automatically converted "
                "to a HyperOpt search space.")

        def resolve_value(par, domain):
            quantize = None

            sampler = domain.get_sampler()
            if isinstance(sampler, Quantized):
                quantize = sampler.q
                sampler = sampler.sampler

            if isinstance(domain, Float):
                if isinstance(sampler, LogUniform):
                    if quantize:
                        return hpo.hp.qloguniform(par, domain.min, domain.max,
                                                  quantize)
                    return hpo.hp.loguniform(par, np.log(domain.min),
                                             np.log(domain.max))
                elif isinstance(sampler, Uniform):
                    if quantize:
                        return hpo.hp.quniform(par, domain.min, domain.max,
                                               quantize)
                    return hpo.hp.uniform(par, domain.min, domain.max)
                elif isinstance(sampler, Normal):
                    if quantize:
                        return hpo.hp.qnormal(par, sampler.mean, sampler.sd,
                                              quantize)
                    return hpo.hp.normal(par, sampler.mean, sampler.sd)

            elif isinstance(domain, Integer):
                if isinstance(sampler, Uniform):
                    if quantize:
                        logger.warning(
                            "HyperOpt does not support quantization for "
                            "integer values. Dropped quantization.")
                    if domain.min != 0:
                        logger.warning(
                            "HyperOpt only allows integer sampling with "
                            "lower bound 0. Dropped the lower bound {}".format(
                                domain.min))
                    if domain.max < 1:
                        raise ValueError(
                            "HyperOpt does not support integer sampling "
                            "of values lower than 0. Set your maximum range "
                            "to something above 0 (currently {})".format(
                                domain.max))
                    return hpo.hp.randint(par, domain.max)
            elif isinstance(domain, Categorical):
                if isinstance(sampler, Uniform):
                    return hpo.hp.choice(par, domain.categories)

            raise ValueError("HyperOpt does not support parameters of type "
                             "`{}` with samplers of type `{}`".format(
                                 type(domain).__name__,
                                 type(domain.sampler).__name__))

        for path, domain in domain_vars:
            par = "/".join(path)
            value = resolve_value(par, domain)
            assign_value(spec, path, value)

        return spec
