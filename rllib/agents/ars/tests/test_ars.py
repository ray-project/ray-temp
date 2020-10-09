import unittest

import ray
import ray.rllib.agents.ars as ars
from ray.rllib.utils.test_utils import framework_iterator, \
    check_compute_single_action


class TestARS(unittest.TestCase):
    def test_ars_compilation(self):
        """Test whether an ARSTrainer can be built on all frameworks."""
        ray.init(num_cpus=2, local_mode=True)
        config = ars.DEFAULT_CONFIG.copy()
        # Keep it simple.
        config["model"]["fcnet_hiddens"] = [10]
        config["model"]["fcnet_activation"] = None
        # Test eval workers ("normal" Trainer eval WorkerSet, unusual for ARS).
        config["evaluation_interval"] = 1
        config["evaluation_num_workers"] = 2

        num_iterations = 2

        for _ in framework_iterator(config):
            plain_config = config.copy()
            trainer = ars.ARSTrainer(config=plain_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

            check_compute_single_action(trainer)
            trainer.stop()
        ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
