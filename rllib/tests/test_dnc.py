import unittest
import torch

import ray
import gym
from ray import tune
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.examples.models.neural_computer import DNCMemory


class TestDNC(unittest.TestCase):

    config = {
    }

    stop = {
        "episode_reward_mean": 150.0,
        "timesteps_total": 5000000,
    }

    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4, ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_pack_unpack(self):
        d = DNCMemory(gym.spaces.Discrete(1), gym.spaces.Discrete(1), 1, {}, "")
        # Add batch dim
        packed_state = [m.unsqueeze(0) for m in d.get_initial_state()]
        [m.random_() for m in packed_state]
        original_packed = [m.clone() for m in packed_state] 

        B, T = packed_state[0].shape[:2]
        unpacked = d.unpack_state(packed_state)
        packed = d.pack_state(*unpacked)

        self.assertTrue(len(packed) > 0)
        self.assertEqual(len(packed), len(original_packed))

        for m_idx in range(len(packed)):
            self.assertTrue(
                torch.all(packed[m_idx] == original_packed[m_idx])
            )
     

    def test_dnc_learning(self):
        ModelCatalog.register_custom_model("dnc", DNCMemory)
        config = {
        "env": StatelessCartPole,
        "gamma": 0.99,
        "num_envs_per_worker": 10,
        "framework": "torch",
            "num_workers": 0,
            "num_gpus": 1,
            "entropy_coeff": 0.0005,
            "lr": 0.01,
            #"vf_loss_coeff": 1e-5,
            #"num_sgd_iter": 5,
            "model": {
                "custom_model": "dnc",
                "max_seq_len": 10,
                "custom_model_config": {
                    "nr_cells": 10,
                    "read_heads": 2,
                    "cell_size": 4,
                    "num_layers": 1,
                    "hidden_size": 64,
                },
            },
        }
        tune.run("IMPALA", config=config, stop=self.stop, verbose=1)

if __name__ == "__main__":
    import pytest
    import sys
    unittest.main()
    #sys.exit(pytest.main(["-v", __file__]))
