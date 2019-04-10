from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import shutil
import random
import os
import pandas as pd

from experiment_analysis import ExperimentAnalysis
import ray
from ray.tune import run, sample_from, Trainable
from ray.tune.examples.async_hyperband_example import MyTrainableClass
from ray.tune.schedulers import AsyncHyperBandScheduler

test_dir = "~/analysis_test"
test_name = "analysis_exp"
test_path = os.path.join(test_dir, test_name)
num_samples = 10
metric = "episode_reward_mean"


def run_test_exp():

    ahb = AsyncHyperBandScheduler(
        time_attr="training_iteration",
        reward_attr=metric,
        grace_period=5,
        max_t=100)

    run(MyTrainableClass,
        name=test_name,
        scheduler=ahb,
        local_dir=test_dir,
        **{
            "stop": {
                "training_iteration": 1
            },
            "num_samples": num_samples,
            "config": {
                "width": sample_from(
                    lambda spec: 10 + int(90 * random.random())),
                "height": sample_from(lambda spec: int(100 * random.random())),
            },
        })


def remove_test_exp():
    shutil.rmtree(os.path.expanduser(test_dir), ignore_errors=True)


class ExperimentAnalysisSuite(unittest.TestCase):
    def setUp(self):
        ray.init()
        run_test_exp()
        self.ea = ExperimentAnalysis(test_path)

    def tearDown(self):
        remove_test_exp()
        ray.shutdown()

    def testDataframe(self):
        df = self.ea.dataframe()

        self.assertTrue(isinstance(df, pd.DataFrame))
        self.assertEquals(df.shape[0], num_samples)

    def testTrialDataframe(self):
        cs = self.ea.checkpoints()
        idx = random.randint(0, len(cs) - 1)
        trial_df = self.ea.trial_dataframe(
            cs[idx]["trial_id"])  # random trial df

        self.assertTrue(isinstance(trial_df, pd.DataFrame))
        self.assertEqual(trial_df.shape[0], 1)

    def testBestTrainable(self):
        best_trainable = self.ea.get_best_trainable(metric)

        self.assertTrue(isinstance(best_trainable, Trainable))

    def testBestConfig(self):
        best_config = self.ea.get_best_config(metric)

        self.assertTrue(isinstance(best_config, dict))
        self.assertTrue("width" in best_config)
        self.assertTrue("height" in best_config)

    def testBestTrial(self):
        best_trial = self.ea.get_best_trial(metric)

        self.assertTrue(isinstance(best_trial, dict))
        self.assertTrue("local_dir" in best_trial)
        self.assertEqual(best_trial["local_dir"],
                         os.path.expanduser(test_path))
        self.assertTrue("config" in best_trial)
        self.assertTrue("width" in best_trial["config"])
        self.assertTrue("height" in best_trial["config"])
        self.assertTrue("last_result" in best_trial)
        self.assertTrue(metric in best_trial["last_result"])

    def testCheckpoints(self):
        checkpoints = self.ea.checkpoints()

        self.assertTrue(isinstance(checkpoints, list))
        self.assertTrue(isinstance(checkpoints[0], dict))
        self.assertEqual(len(checkpoints), num_samples)

    def testStats(self):
        stats = self.ea.stats()

        self.assertTrue(isinstance(stats, dict))
        self.assertTrue("start_time" in stats)
        self.assertTrue("timestamp" in stats)

    def testRunnerData(self):
        runner_data = self.ea.runner_data()

        self.assertTrue(isinstance(runner_data, dict))
        self.assertTrue("_metadata_checkpoint_dir" in runner_data)
        self.assertEqual(runner_data["_metadata_checkpoint_dir"],
                         os.path.expanduser(test_path))


if __name__ == "__main__":
    unittest.main(verbosity=2)
