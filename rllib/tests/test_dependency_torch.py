#!/usr/bin/env python

import os
import sys


if __name__ == "__main__":
    import tensorflow as tf
    import re
    tf_file = tf.__file__
    torch_file = re.sub("tensorflow", "torch", tf_file)
    print(torch_file)
    assert os.path.isfile(torch_file)
    with open(torch_file, "w") as f:
        print("raise Exception('torch imported!')", file=f)

    # Do not import tf for testing purposes.
    os.environ["RLLIB_TEST_NO_TORCH_IMPORT"] = "1"

    from ray.rllib.agents.a3c import A2CTrainer
    #if "torch" in sys.modules:
        #import inspect
        #print(inspect.getframeinfo(inspect.getouterframes(inspect.currentframe())[1][0])[0])
    assert "torch" not in sys.modules, \
        "PyTorch initially present, when it shouldn't!"

    # note: no ray.init(), to test it works without Ray
    trainer = A2CTrainer(
        env="CartPole-v0", config={
            "framework": "tf",
            "num_workers": 0
        })
    trainer.train()

    assert "torch" not in sys.modules, \
        "PyTorch should not be imported after creating and " \
        "training A3C agent!"

    # Clean up.
    del os.environ["RLLIB_TEST_NO_TORCH_IMPORT"]

    print("ok")
