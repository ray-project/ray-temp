from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib.util
import os
import sys
import time
import threading

from ray.rllib.common import Agent


class StatusReporter(object):
    """Object passed into your main() that you can report status through."""

    def __init__(self):
        self._latest_result = None
        self._lock = threading.Lock()
        self._error = None

    def report(self, result):
        """Report updated training status.

        Args:
            result (TrainingResult): Latest training result status. You must
                at least define `timesteps_total`, but probably want to report
                some of the other metrics as well.
        """

        with self._lock:
            self._latest_result = result

    def set_error(self, error):
        """Report an error.

        Args:
            error (obj): Error object or string.
        """

        self._error = error

    def _get_and_clear_status(self):
        if self._error:
            raise Exception("Error running script: " + str(self._error))
        with self._lock:
            res = self._latest_result
            self._latest_result = None
            return res


DEFAULT_CONFIG = {
    # path of the script to run
    "script_file_path": "/path/to/file.py",

    # main function in the file, e.g. main(config, status_reporter)
    "script_entrypoint": "main",

    # batch results to at least this granularity
    "script_min_iter_time_s": 10,
}


class _RunnerThread(threading.Thread):
    """Supervisor thread that runs your script."""

    def __init__(self, entrypoint, config, status_reporter):
        self._entrypoint = entrypoint
        self._entrypoint_args = [config, status_reporter]
        self._status_reporter = status_reporter
        threading.Thread.__init__(self)

    def run(self):
        try:
            self._entrypoint(*self._entrypoint_args)
        except Exception as e:
            self._status_reporter.set_error(e)
            raise e


class _ScriptRunner(Agent):
    """Agent that runs a user script that returns training results.

    Note that you probably want to use trial.PythonScriptTrial instead of
    constructing this agent directly."""

    _agent_name = "script"
    _default_config = DEFAULT_CONFIG
    _allow_unknown_configs = True

    def _init(self):
        # strong assumption here that we're in a new process
        file_path = os.path.expanduser(self.config["script_file_path"])
        sys.path.insert(0, os.path.dirname(file_path))
        spec = importlib.util.spec_from_file_location(
            "external_file", file_path)
        foo = importlib.util.module_from_spec(spec)
        if not foo:
            raise Exception(
                "Unable to import file at {}".format(
                    self.config["script_file_path"]))
        spec.loader.exec_module(foo)
        entrypoint = getattr(foo, self.config["script_entrypoint"])
        self._status_reporter = StatusReporter()
        self._runner = _RunnerThread(
            entrypoint, self.config, self._status_reporter)
        self._start_time = time.time()
        self._last_reported_time = self._start_time
        self._last_reported_timestep = 0
        self._runner.start()

    def train(self):
        poll_start = time.time()
        result = self._status_reporter._get_and_clear_status()
        while result is None or \
                time.time() - poll_start < \
                self.config["script_min_iter_time_s"]:
            time.sleep(1)
            result = self._status_reporter._get_and_clear_status()

        now = time.time()

        # Include the negative loss to use as a stopping condition
        if result.mean_loss is not None:
            neg_loss = -result.mean_loss
        else:
            neg_loss = result.neg_mean_loss

        result = result._replace(
            experiment_id=self._experiment_id,
            neg_mean_loss=neg_loss,
            training_iteration=self.iteration,
            time_this_iter_s=now - self._last_reported_time,
            timesteps_this_iter=(
                result.timesteps_total - self._last_reported_timestep),
            time_total_s=now - self._start_time)

        if result.timesteps_total:
            self._last_reported_timestep = result.timesteps_total
        self._last_reported_time = now
        self._iteration += 1
        self._log_result(result)

        return result
