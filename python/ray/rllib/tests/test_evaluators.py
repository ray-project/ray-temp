from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib.agents.dqn import DQNTrainer
from ray.rllib.agents.a3c import A3CTrainer
from ray.rllib.agents.dqn.dqn_policy_graph import _adjust_nstep


class EvalTest(unittest.TestCase):
    def testDqnNStep(self):
        obs = [1, 2, 3, 4, 5, 6, 7]
        actions = ["a", "b", "a", "a", "a", "b", "a"]
        rewards = [10.0, 0.0, 100.0, 100.0, 100.0, 100.0, 100.0]
        new_obs = [2, 3, 4, 5, 6, 7, 8]
        dones = [0, 0, 0, 0, 0, 0, 1]
        _adjust_nstep(3, 0.9, obs, actions, rewards, new_obs, dones)
        self.assertEqual(obs, [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(actions, ["a", "b", "a", "a", "a", "b", "a"])
        self.assertEqual(new_obs, [4, 5, 6, 7, 8, 8, 8])
        self.assertEqual(dones, [0, 0, 0, 0, 1, 1, 1])
        self.assertEqual(rewards,
                         [91.0, 171.0, 271.0, 271.0, 271.0, 190.0, 100.0])

    def testEvaluationOption(self):
        ray.init()
        agent_classes = [DQNTrainer, A3CTrainer]
        for agent_cls in agent_classes:
            agent = agent_cls(
                env="CartPole-v0",
                config={"evaluation_interval": 2, "evaluation_num_episodes": 2})
            # Given evaluation_interval=2, r0, r2, r4 should not contain
            # evaluation metrics while r1, r3 should do.
            r0 = agent.train()
            r1 = agent.train()
            r2 = agent.train()
            r3 = agent.train()

            self.assertTrue("evaluation" in r1)
            self.assertTrue("evaluation" in r3)
            self.assertFalse("evaluation" in r0)
            self.assertFalse("evaluation" in r2)
            self.assertTrue("episode_reward_mean" in r1["evaluation"])
            self.assertNotEqual(r1["evaluation"], r3["evaluation"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
