import os
import tempfile
from collections import namedtuple
from multiprocessing import Queue
import unittest

from ray.tune.integration.wandb import _WandbLoggingProcess, \
    _WANDB_QUEUE_END, WandbLogger, WANDB_ENV_VAR

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


class WandbLoggerTest(unittest.TestCase):
    """Test built-in loggers."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testWandbConfig(self):
        trial_config = {"par1": 4, "par2": 9.12345678}
        trial = Trial(trial_config, 0, "trial_0", "trainable")

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

        # Group by one parameter, log config.
        trial_config["wandb"] = {
            "project": "test_project",
            "group_by": ["par1"],
            "log_config": True
        }

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertEqual(logger._wandb.kwargs["group"], "par1=4")
        self.assertNotIn("config", logger._wandb._exclude)
        self.assertNotIn("metric", logger._wandb._exclude)

        logger.close()

        # Group by two parameters, exclude metrics.
        trial_config["wandb"] = {
            "project": "test_project",
            "group_by": ["par1", "par2"],
            "excludes": ["metric"]
        }

        logger = WandbTestLogger(trial_config, "/tmp", trial)
        self.assertEqual(logger._wandb.kwargs["group"], "par1=4,par2=9.123457")
        self.assertIn("config", logger._wandb._exclude)
        self.assertIn("metric", logger._wandb._exclude)

        logger.close()

        # Invalid group by
        trial_config["wandb"] = {
            "project": "test_project",
            "group_by": ["par1", "invalid"]
        }
        with self.assertRaises(ValueError):
            logger = WandbTestLogger(trial_config, "/tmp", trial)

        # Invalid group by
        trial_config["wandb"] = {
            "project": "test_project",
            "group_by": "invalid"
        }
        with self.assertRaises(ValueError):
            logger = WandbTestLogger(trial_config, "/tmp", trial)

    def testReporting(self):
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
            "const": "text",
            "config": trial_config
        }

        logger.on_result(r1)

        logged = logger._wandb.logs.get(timeout=10)
        self.assertIn("metric1", logged)
        self.assertNotIn("metric2", logged)
        self.assertNotIn("const", logged)
        self.assertNotIn("config", logged)

        logger.close()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
