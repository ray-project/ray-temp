from pathlib import Path
import os
import re
import unittest

import ray
from ray.rllib.utils.test_utils import framework_iterator


def rollout_test(algo, env="CartPole-v0", test_episode_rollout=False):
    extra_config = ""
    if algo == "ARS":
        extra_config = ",\"train_batch_size\": 10, \"noise_size\": 250000"
    elif algo == "ES":
        extra_config = ",\"episodes_per_batch\": 1,\"train_batch_size\": 10, "\
                       "\"noise_size\": 250000"

    for fw in framework_iterator(frameworks=("tf", "torch")):
        fw_ = ", \"framework\": \"{}\"".format(fw)

        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            sys.exit(1)

        print("Saving results to {}".format(tmp_dir))

        rllib_dir = str(Path(__file__).parent.parent.absolute())
        print("RLlib dir = {}\nexists={}".format(rllib_dir,
                                                 os.path.exists(rllib_dir)))
        os.system("python {}/train.py --local-dir={} --run={} "
                  "--checkpoint-freq=1 ".format(rllib_dir, tmp_dir, algo) +
                  "--config='{" + "\"num_workers\": 1, \"num_gpus\": 0{}{}".
                  format(fw_, extra_config) +
                  ", \"timesteps_per_iteration\": 5,\"min_iter_time_s\": 0.1, "
                  "\"model\": {\"fcnet_hiddens\": [10]}"
                  "}' --stop='{\"training_iteration\": 1}'" +
                  " --env={}".format(env))

        checkpoint_path = os.popen("ls {}/default/*/checkpoint_1/"
                                   "checkpoint-1".format(tmp_dir)).read()[:-1]
        if not os.path.exists(checkpoint_path):
            sys.exit(1)
        print("Checkpoint path {} (exists)".format(checkpoint_path))

        # Test rolling out n steps.
        os.popen("python {}/rollout.py --run={} \"{}\" --steps=10 "
                 "--out=\"{}/rollouts_10steps.pkl\" --no-render".format(
                     rllib_dir, algo, checkpoint_path, tmp_dir)).read()
        if not os.path.exists(tmp_dir + "/rollouts_10steps.pkl"):
            sys.exit(1)
        print("rollout output (10 steps) exists!".format(checkpoint_path))

        # Test rolling out 1 episode.
        if test_episode_rollout:
            os.popen("python {}/rollout.py --run={} \"{}\" --episodes=1 "
                     "--out=\"{}/rollouts_1episode.pkl\" --no-render".format(
                         rllib_dir, algo, checkpoint_path, tmp_dir)).read()
            if not os.path.exists(tmp_dir + "/rollouts_1episode.pkl"):
                sys.exit(1)
            print("rollout output (1 ep) exists!".format(checkpoint_path))

        # Cleanup.
        os.popen("rm -rf \"{}\"".format(tmp_dir)).read()


def learn_test_plus_rollout(algo, env="CartPole-v0"):
    for fw in framework_iterator(frameworks="tf"):
        fw_ = ", \\\"framework\\\": \\\"{}\\\"".format(fw)

        tmp_dir = os.popen("mktemp -d").read()[:-1]
        if not os.path.exists(tmp_dir):
            # Last resort: Resolve via underlying tempdir (and cut tmp_.
            tmp_dir = ray.utils.tempfile.gettempdir() + tmp_dir[4:]
            if not os.path.exists(tmp_dir):
                sys.exit(1)

        print("Saving results to {}".format(tmp_dir))

        rllib_dir = str(Path(__file__).parent.parent.absolute())
        print("RLlib dir = {}\nexists={}".format(rllib_dir,
                                                 os.path.exists(rllib_dir)))
        os.system("python {}/train.py --local-dir={} --run={} "
                  "--checkpoint-freq=1 ".format(rllib_dir, tmp_dir, algo) +
                  "--config=\"{\\\"num_gpus\\\": 0" + fw_ + "}\" " +
                  "--stop=\"{\\\"episode_reward_mean\\\": 190}\"" +
                  " --env={}".format(env))

        # Find last checkpoint and use that for the rollout.
        checkpoint_path = os.popen("ls {}/default/*/checkpoint_*/"
                                   "checkpoint-*".format(tmp_dir)).read()[:-1]
        checkpoints = checkpoint_path.split("\n")
        # -1 is tune_metadata
        last_checkpoint = checkpoints[-2]
        assert re.match(r"^.+checkpoint_\d+/checkpoint-\d+$", last_checkpoint)
        if not os.path.exists(last_checkpoint):
            sys.exit(1)
        print("Checkpoint path {} (exists)".format(checkpoint_path))

        # Test rolling out n steps.
        result = os.popen(
            "python {}/rollout.py --run={} \"{}\" "
            "--steps=400 "
            "--out=\"{}/rollouts_n_steps.pkl\" --no-render {}".format(
                rllib_dir, algo, checkpoint_path, tmp_dir, last_checkpoint)
        ).read()[:-1]
        if not os.path.exists(tmp_dir + "/rollouts_n_steps.pkl"):
            sys.exit(1)
        print("rollout output exists!".format(checkpoint_path))
        episodes = result.split("\n")
        for ep in episodes:
            mo = re.match(r"Episode: ", result)
            if mo:
                assert float(mo.group(1)) > 150.0

        # Cleanup.
        os.popen("rm -rf \"{}\"".format(tmp_dir)).read()


class TestRolloutSimple(unittest.TestCase):
    def test_a3c(self):
        rollout_test("A3C")

    def test_ddpg(self):
        rollout_test("DDPG", env="Pendulum-v0")

    def test_dqn(self):
        rollout_test("DQN")

    def test_es(self):
        rollout_test("ES")

    def test_impala(self):
        rollout_test("IMPALA", env="CartPole-v0")

    def test_ppo(self):
        rollout_test("PPO", env="CartPole-v0", test_episode_rollout=True)

    def test_sac(self):
        rollout_test("SAC", env="Pendulum-v0")


class TestRolloutLearntPolicy(unittest.TestCase):
    def test_ppo_train_then_rollout(self):
        learn_test_plus_rollout("PPO")


if __name__ == "__main__":
    import sys
    import pytest

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(
        ["-v", __file__ + ("" if class_ is None else "::" + class_)]))
