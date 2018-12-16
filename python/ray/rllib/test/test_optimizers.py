from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import numpy as np

import ray
from ray.rllib.agents.ppo import PPOAgent
from ray.rllib.test.mock_evaluator import _MockEvaluator
from ray.rllib.optimizers import AsyncGradientsOptimizer
from ray.rllib.evaluation import SampleBatch


class AsyncOptimizerTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testBasic(self):
        ray.init(num_cpus=4)
        local = _MockEvaluator()
        remotes = ray.remote(_MockEvaluator)
        remote_evaluators = [remotes.remote() for i in range(5)]
        test_optimizer = AsyncGradientsOptimizer(local, remote_evaluators,
                                                 {"grads_per_step": 10})
        test_optimizer.step()
        self.assertTrue(all(local.get_weights() == 0))


class PPOCollectTest(unittest.TestCase):
    def tearDown(self):
        ray.shutdown()

    def testPPOSampleWaste(self):
        ray.init(num_cpus=4)

        # Check we at least collect the initial wave of samples
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "train_batch_size": 128,
                "num_workers": 3,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 600)
        ppo.stop()

        # Check we collect at least the specified amount of samples
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "train_batch_size": 900,
                "num_workers": 3,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 1000)
        ppo.stop()

        # We will wait for all sample requests to complete.
        ppo = PPOAgent(
            env="CartPole-v0",
            config={
                "sample_batch_size": 200,
                "train_batch_size": 900,
                "num_workers": 3,
                "wait_for_stragglers": True,
            })
        ppo.train()
        self.assertEqual(ppo.optimizer.num_steps_sampled, 1400)
        ppo.stop()


class SampleBatchTest(unittest.TestCase):
    def testConcat(self):
        b1 = SampleBatch({"a": np.array([1, 2, 3]), "b": np.array([4, 5, 6])})
        b2 = SampleBatch({"a": np.array([1]), "b": np.array([4])})
        b3 = SampleBatch({"a": np.array([1]), "b": np.array([5])})
        b12 = b1.concat(b2)
        self.assertEqual(b12["a"].tolist(), [1, 2, 3, 1])
        self.assertEqual(b12["b"].tolist(), [4, 5, 6, 4])
        b = SampleBatch.concat_samples([b1, b2, b3])
        self.assertEqual(b["a"].tolist(), [1, 2, 3, 1, 1])
        self.assertEqual(b["b"].tolist(), [4, 5, 6, 4, 5])


if __name__ == '__main__':
    unittest.main(verbosity=2)
