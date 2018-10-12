from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle

from gym import spaces
from gym.envs.registration import EnvSpec
import gym
import tensorflow.contrib.slim as slim
import tensorflow as tf
import unittest

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.models import ModelCatalog
from ray.rllib.models.model import Model
from ray.tune.registry import register_env

NESTED_SPACE = spaces.Dict({
    "sensors": spaces.Dict({
        "position": spaces.Box(low=-100, high=100, shape=(3, )),
        "velocity": spaces.Box(low=-1, high=1, shape=(3, )),
        "front_cam": spaces.Tuple(
            (spaces.Box(low=0, high=1, shape=(10, 10, 3)),
             spaces.Box(low=0, high=1, shape=(10, 10, 3)))),
        "rear_cam": spaces.Box(low=0, high=1, shape=(10, 10, 3)),
    }),
    "inner_state": spaces.Dict({
        "charge": spaces.Discrete(100),
        "job_status": spaces.Dict({
            "task": spaces.Discrete(5),
            "progress": spaces.Box(low=0, high=100, shape=()),
        })
    })
})

SAMPLES = [NESTED_SPACE.sample() for _ in range(10)]


class NestedEnv(gym.Env):
    def __init__(self):
        self.action_space = spaces.Discrete(2)
        self.observation_space = NESTED_SPACE
        self._spec = EnvSpec("NestedEnv-v0")
        self.steps = 0

    def reset(self):
        self.steps = 0
        return SAMPLES[0]

    def step(self, action):
        self.steps += 1
        return SAMPLES[self.steps], 1, self.steps >= 5, {}


class InvalidModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        return "not", "valid"


class SpyModel(Model):
    capture_index = 0

    def _build_layers_v2(self, input_dict, num_outputs, options):
        def spy(pos, front_cam, task):
            ray.experimental.internal_kv._internal_kv_put(
                "spy_in_{}".format(SpyModel.capture_index),
                pickle.dumps((pos, front_cam, task)))
            SpyModel.capture_index += 1
            return 0

        spy_fn = tf.py_func(
            spy, [
                input_dict["obs"]["sensors"]["position"],
                input_dict["obs"]["sensors"]["front_cam"][0],
                input_dict["obs"]["inner_state"]["job_status"]["task"]
            ],
            tf.int64,
            stateful=True)

        with tf.control_dependencies([spy_fn]):
            output = slim.fully_connected(
                input_dict["obs"]["sensors"]["position"], num_outputs)
        return output, output


class NestedSpacesTest(unittest.TestCase):
    def testInvalidModel(self):
        ModelCatalog.register_custom_model("invalid", InvalidModel)
        self.assertRaises(ValueError, lambda: PGAgent(
            env="CartPole-v0", config={
                "model": {
                    "custom_model": "invalid",
                },
            }))

    def testNested(self):
        ModelCatalog.register_custom_model("composite", SpyModel)
        register_env("nested", lambda _: NestedEnv())
        pg = PGAgent(
            env="nested",
            config={
                "num_workers": 0,
                "sample_batch_size": 5,
                "model": {
                    "custom_model": "composite",
                },
            })
        pg.train()
        batch = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("spy_in_5"))

        def one_hot(i, n):
            out = [0.0] * n
            out[i] = 1.0
            return out

        # Check that the model sees the correct reconstructed observations
        for i in range(4):
            seen = pickle.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    "spy_in_{}".format(i)))
            pos_i = SAMPLES[i]["sensors"]["position"].tolist()
            cam_i = SAMPLES[i]["sensors"]["front_cam"][0].tolist()
            task_i = one_hot(SAMPLES[i]["inner_state"]["job_status"]["task"],
                             5)
            self.assertEqual(seen[0][0].tolist(), pos_i)
            self.assertEqual(seen[1][0].tolist(), cam_i)
            self.assertEqual(seen[2][0].tolist(), task_i)
            self.assertEqual(batch[0][i].tolist(), pos_i)
            self.assertEqual(batch[1][i].tolist(), cam_i)
            self.assertEqual(batch[2][i].tolist(), task_i)


if __name__ == "__main__":
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
