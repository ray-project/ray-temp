from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hyperopt as hpo
from ray.tune.config_parser import make_parser
from ray.tune.variant_generator import to_argv
import time
import copy
import numpy as np
from ray.tune.trial import Trial
from ray.tune import TuneError
from ray.tune import register_trainable
from ray.tune.trial_scheduler import TrialScheduler

from ray.tune.trial_scheduler import FIFOScheduler
from hyperopt import tpe, Domain, Trials


def easy_objective(config, reporter):

    # val = config["height"]
    time.sleep(0.2)
    reporter(
        timesteps_total=1,
        mean_loss=((config["height"] - 14) ** 2 + abs(config["width"] - 3)))
    time.sleep(0.2)


class HyperOptScheduler(FIFOScheduler):

    def __init__(self, max_concurrent=10, loss_attr="mean_loss"):
        self._max_concurrent = max_concurrent  # NOTE: this is modified later
        self._loss_attr = loss_attr
        self._experiment = None

    def track_experiment(self, experiment, trial_runner):
        assert self._experiment is None, "HyperOpt only tracks one experiment!"
        self._experiment = experiment

        name = experiment.name  # TODO(rliaw) Find out where name is to be used
        spec = copy.deepcopy(experiment.spec)
        if "env" in spec:
            spec["config"] = spec.get("config", {})
            spec["config"]["env"] = spec["env"]
            del spec["env"]

        space = spec["config"]["space"]
        del spec["config"]["space"]

        self.parser = make_parser()
        self.args = self.parser.parse_args(to_argv(spec))
        self.default_config = copy.deepcopy(spec["config"])

        self.algo = tpe.suggest
        self.domain = Domain(lambda spec: spec, space)
        self._hpopt_trials = Trials()
        self._tune_to_hp = {}
        self._num_trials_left = self.args.repeat

        self._max_concurrent = min(self._max_concurrent, self.args.repeat)
        self.rstate = np.random.RandomState()
        self.trial_generator = self._trial_generator()
        self.refresh_queue(trial_runner)

    def _trial_generator(self):
        while self._num_trials_left > 0:
            new_cfg = copy.deepcopy(self.default_config)
            new_ids = self._hpopt_trials.new_trial_ids(1)
            self._hpopt_trials.refresh()

            new_trials = self.algo(
                new_ids, self.domain, self._hpopt_trials,
                self.rstate.randint(2 ** 31 - 1))

            self._hpopt_trials.insert_trial_docs(new_trials)
            self._hpopt_trials.refresh()
            new_trial = new_trials[0]
            new_trial_id = new_trial["tid"]

            suggested_config = hpo.base.spec_from_misc(new_trial["misc"])
            new_cfg.update(suggested_config)
            kv_str = "_".join(["{}={}".format(k, str(v)[:5])
                               for k, v in suggested_config.items()])
            experiment_tag = "hyperopt_{}_{}".format(new_trial_id, kv_str)

            trial = Trial(
                trainable_name=self.args.run,
                config=new_cfg,
                local_dir=self.args.local_dir,
                experiment_tag=experiment_tag,
                resources=self.args.resources,
                stopping_criterion=self.args.stop,
                checkpoint_freq=self.args.checkpoint_freq,
                restore_path=self.args.restore,
                upload_dir=self.args.upload_dir)

            self._tune_to_hp[trial] = new_trial_id
            self._num_trials_left -= 1
            yield trial

    def on_trial_result(self, trial_runner, trial, result):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        now = hpo.utils.coarse_utcnow()
        ho_trial['book_time'] = now
        ho_trial['refresh_time'] = now
        return TrialScheduler.CONTINUE

    def on_trial_error(self, trial_runner, trial):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Error")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def on_trial_remove(self, trial_runner, trial):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_ERROR
        ho_trial['misc']['error'] = (str(TuneError), "Tune Removed")
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def on_trial_complete(self, trial_runner, trial, result):
        ho_trial = self._get_dynamic_trial(self._tune_to_hp[trial])
        ho_trial['refresh_time'] = hpo.utils.coarse_utcnow()
        ho_trial['state'] = hpo.base.JOB_STATE_DONE
        hp_result = self._convert_result(result)
        ho_trial['result'] = hp_result
        self._hpopt_trials.refresh()
        del self._tune_to_hp[trial]

    def _convert_result(self, result):
        return {"loss": getattr(result, self._loss_attr),
                "status": "ok"}

    def _get_dynamic_trial(self, tid):
        return [t for t in self._hpopt_trials.trials if t["tid"] == tid][0]

    def _continue(self):
        return self._num_trials_left > 0

    def get_hyperopt_trials(self):
        return self._hpopt_trials

    def choose_trial_to_run(self, trial_runner):
        self.refresh_queue(trial_runner)
        FIFOScheduler.choose_trial_to_run(self, trial_runner)

    def refresh_queue(self, trial_runner):
        """Checks if there is a next trial ready to be queued.

        This is determined by tracking the number of concurrent
        experiments and trials left to run."""
        while (self._num_trials_left > 0 and
               self._num_live_trials() < self._max_concurrent):
            try:
                trial_runner.add_trial(next(self.trial_generator))
            except StopIteration:
                break

    def _num_live_trials(self):
        return len(self._tune_to_hp)


if __name__ == '__main__':
    import ray
    from hyperopt import hp
    ray.init(redirect_output=True)
    from ray.tune import run_experiments
    # register_trainable("exp", MyTrainableClass)

    register_trainable("exp", easy_objective)

    space = {
        'width': hp.uniform('width', 0, 20),
        'height': hp.uniform('height', -100, 100),
    }

    config = {"my_exp": {
            "run": "exp",
            "repeat": 1000,
            "stop": {"training_iteration": 1},
            "config": {
                "space": space}}}
    run_experiments(config, verbose=False)
