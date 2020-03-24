from tensorflow.python.eager.context import eager_mode
import unittest

import ray
import ray.rllib.agents.sac as sac
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class TestSAC(unittest.TestCase):
    def test_sac_compilation(self):
        """Test whether an SACTrainer can be built with all frameworks."""
        ray.init()
        config = sac.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        num_iterations = 1

        # eager (discrete and cont. actions).
        for fw in ["torch", "eager", "tf"]:
            print("framework={}".format(fw))

            config["eager"] = fw == "eager"
            config["use_pytorch"] = fw == "torch"

            eager_ctx = None
            if fw == "eager":
                eager_ctx = eager_mode()
                eager_ctx.__enter__()
                assert tf.executing_eagerly()
            elif fw == "tf":
                assert not tf.executing_eagerly()

            for env in [
                    "CartPole-v0",
                    "Pendulum-v0",
            ]:
                print("Env={}".format(env))
                trainer = sac.SACTrainer(config=config, env=env)
                for i in range(num_iterations):
                    results = trainer.train()
                    print(results)

            if eager_ctx:
                eager_ctx.__exit__(None, None, None)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
