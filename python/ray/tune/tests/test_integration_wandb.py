import os
import tempfile
from collections import namedtuple
from multiprocessing import Queue
import unittest

import numpy as np

from ray.tune import Trainable
from ray.tune.function_runner import wrap_function
from ray.tune.integration.wandb import _WandbLoggingProcess, \
    _WANDB_QUEUE_END, WandbLogger, WANDB_ENV_VAR, WandbTrainableMixin, \
    wandb_mixin
from ray.tune.result import TRIAL_INFO
from ray.tune.trial import TrialInfo

Trial = namedtuple("MockTrial",
                   ["config", "trial_id", "trial_name", "trainable_name"])
Trial.__str__ = lambda t: t.trial_name


class _MockWandbLoggingProcess(_WandbLoggingProcess):
    def __init__(self, queue, exclude, to_config, *args, **kwargs):
        super(_MockWandbLoggingProcess,
              self).__init__(queue, exclude, to_config, *args, **kwargs)

        self.logs = Queue()
        self.config_updates = Queue()

    def run(self):
        while True:
            result = self.queue.get()
            if result == _WANDB_QUEUE_END:
                break
            log, config_update = self._handle_result(result)
            self.config_updates.put(config_update)
            self.logs.put(log)


class WandbTestLogger(WandbLogger):
    _logger_process_cls = _MockWandbLoggingProcess


class _MockWandbAPI(object):
    def init(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self


class _MockWandbTrainableMixin(WandbTrainableMixin):
    _wandb = _MockWandbAPI()


class WandbTestTrainable(_MockWandbTrainableMixin, Trainable):
    pass


class WandbIntegrationTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testWandbLoggerConfig(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(trial_config, 0, "trial_0", "trainable")

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # Needs at least a project
        with self.assertRaises(ValueError):
            logger = WandbTestLogger(trial_config, "/tmp", trial)

        # No API key
        trial_config["wandb"] = {"project": "test_project"}
        with self.assertRaises(ValueError):
            logger = WandbTestLogger(trial_config, "/tmp", trial)

        # API Key in config
        trial_config["wandb"] = {"project": "test_project", "api_key": "1234"}
        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        logger.close()
        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            trial_config["wandb"] = {
                "project": "test_project",
                "api_key_file": fp.name
            }

            logger = WandbTestLogger(trial_config, "/tmp", trial)
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        logger.close()
        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        trial_config["wandb"] = {"project": "test_project"}
        logger = WandbTestLogger(trial_config, "/tmp", trial)
        logger.close()

        # From now on, the API key is in the env variable.

        # Default configuration
        trial_config["wandb"] = {"project": "test_project"}

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertEqual(logger._wandb.kwargs["project"], "test_project")
        self.assertEqual(logger._wandb.kwargs["id"], trial.trial_id)
        self.assertEqual(logger._wandb.kwargs["name"], trial.trial_name)
        self.assertEqual(logger._wandb.kwargs["group"], trial.trainable_name)
        self.assertIn("config", logger._wandb._exclude)

        logger.close()

        # log config.
        trial_config["wandb"] = {"project": "test_project", "log_config": True}

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertNotIn("config", logger._wandb._exclude)
        self.assertNotIn("metric", logger._wandb._exclude)

        logger.close()

        # Exclude metric.
        trial_config["wandb"] = {
            "project": "test_project",
            "excludes": ["metric"]
        }

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertIn("config", logger._wandb._exclude)
        self.assertIn("metric", logger._wandb._exclude)

        logger.close()

    def testWandbLoggerReporting(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(trial_config, 0, "trial_0", "trainable")

        trial_config["wandb"] = {
            "project": "test_project",
            "api_key": "1234",
            "excludes": ["metric2"]
        }
        logger = WandbTestLogger(trial_config, "/tmp", trial)

        r1 = {
            "metric1": 0.8,
            "metric2": 1.4,
            "metric3": np.asarray(32.0),
            "metric4": np.float32(32.0),
            "const": "text",
            "config": trial_config
        }

        logger.on_result(r1)

        logged = logger._wandb.logs.get(timeout=10)
        self.assertIn("metric1", logged)
        self.assertNotIn("metric2", logged)
        self.assertIn("metric3", logged)
        self.assertIn("metric4", logged)
        self.assertNotIn("const", logged)
        self.assertNotIn("config", logged)

        logger.close()

    def testWandbMixinConfig(self):
        config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(config, 0, "trial_0", "trainable")
        trial_info = TrialInfo(trial)

        config[TRIAL_INFO] = trial_info

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # Needs at least a project
        with self.assertRaises(ValueError):
            trainable = WandbTestTrainable(config)

        # No API key
        config["wandb"] = {"project": "test_project"}
        with self.assertRaises(ValueError):
            trainable = WandbTestTrainable(config)

        # API Key in config
        config["wandb"] = {"project": "test_project", "api_key": "1234"}
        trainable = WandbTestTrainable(config)
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            config["wandb"] = {
                "project": "test_project",
                "api_key_file": fp.name
            }

            trainable = WandbTestTrainable(config)
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        config["wandb"] = {"project": "test_project"}
        trainable = WandbTestTrainable(config)

        # From now on, the API key is in the env variable.

        # Default configuration
        config["wandb"] = {"project": "test_project"}
        config[TRIAL_INFO] = trial_info

        trainable = WandbTestTrainable(config)
        self.assertEqual(trainable.wandb.kwargs["project"], "test_project")
        self.assertEqual(trainable.wandb.kwargs["id"], trial.trial_id)
        self.assertEqual(trainable.wandb.kwargs["name"], trial.trial_name)
        self.assertEqual(trainable.wandb.kwargs["group"], "WandbTestTrainable")

    def testWandbDecoratorConfig(self):
        config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(config, 0, "trial_0", "trainable")
        trial_info = TrialInfo(trial)

        @wandb_mixin
        def train_fn(config):
            return 1

        train_fn.__mixins__ = (_MockWandbTrainableMixin, )

        config[TRIAL_INFO] = trial_info

        if WANDB_ENV_VAR in os.environ:
            del os.environ[WANDB_ENV_VAR]

        # Needs at least a project
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(config)

        # No API key
        config["wandb"] = {"project": "test_project"}
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(config)

        # API Key in config
        config["wandb"] = {"project": "test_project", "api_key": "1234"}
        wrapped = wrap_function(train_fn)(config)
        self.assertEqual(os.environ[WANDB_ENV_VAR], "1234")

        del os.environ[WANDB_ENV_VAR]

        # API Key file
        with tempfile.NamedTemporaryFile("wt") as fp:
            fp.write("5678")
            fp.flush()

            config["wandb"] = {
                "project": "test_project",
                "api_key_file": fp.name
            }

            wrapped = wrap_function(train_fn)(config)
            self.assertEqual(os.environ[WANDB_ENV_VAR], "5678")

        del os.environ[WANDB_ENV_VAR]

        # API Key in env
        os.environ[WANDB_ENV_VAR] = "9012"
        config["wandb"] = {"project": "test_project"}
        wrapped = wrap_function(train_fn)(config)

        # From now on, the API key is in the env variable.

        # Default configuration
        config["wandb"] = {"project": "test_project"}
        config[TRIAL_INFO] = trial_info

        wrapped = wrap_function(train_fn)(config)
        self.assertEqual(wrapped.wandb.kwargs["project"], "test_project")
        self.assertEqual(wrapped.wandb.kwargs["id"], trial.trial_id)
        self.assertEqual(wrapped.wandb.kwargs["name"], trial.trial_name)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
