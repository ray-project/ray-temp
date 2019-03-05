from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import pickle
import unittest
import tensorflow as tf
import tensorflow.contrib.rnn as rnn

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.models import ModelCatalog
from ray.rllib.models.lstm import add_time_dimension, chop_into_sequences
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.models.model import Model
from ray.tune.registry import register_env


class LSTMUtilsTest(unittest.TestCase):
    def testBasic(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids, agent_ids, f, s,
                                                      4)
        self.assertEqual([f.tolist() for f in f_pad], [
            [101, 102, 103, 0, 201, 202, 203, 204, 205, 0, 0, 0],
            [[101], [102], [103], [0], [201], [202], [203], [204], [205], [0],
             [0], [0]],
        ])
        self.assertEqual([s.tolist() for s in s_init], [[209, 109, 105]])
        self.assertEqual(seq_lens.tolist(), [3, 4, 1])

    def testMultiAgent(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 2, 1, 1, 2, 2, 3]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            eps_ids, agent_ids, f, s, 4, dynamic_max=False)
        self.assertEqual(seq_lens.tolist(), [2, 1, 2, 2, 1])
        self.assertEqual(len(f_pad[0]), 20)
        self.assertEqual(len(s_init[0]), 5)

    def testDynamicMaxLen(self):
        eps_ids = [5, 2, 2]
        agent_ids = [2, 2, 2]
        f = [[1, 1, 1]]
        s = [[1, 1, 1]]
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids, agent_ids, f, s,
                                                      4)
        self.assertEqual([f.tolist() for f in f_pad], [[1, 0, 1, 1]])
        self.assertEqual([s.tolist() for s in s_init], [[1, 1]])
        self.assertEqual(seq_lens.tolist(), [1, 2])


class RNNSpyModel(Model):
    capture_index = 0

    def _build_layers_v2(self, input_dict, num_outputs, options):
        def spy(sequences, state_in, state_out, seq_lens):
            if len(sequences) == 1:
                return 0  # don't capture inference inputs
            # TF runs this function in an isolated context, so we have to use
            # redis to communicate back to our suite
            ray.experimental.internal_kv._internal_kv_put(
                "rnn_spy_in_{}".format(RNNSpyModel.capture_index),
                pickle.dumps({
                    "sequences": sequences,
                    "state_in": state_in,
                    "state_out": state_out,
                    "seq_lens": seq_lens
                }),
                overwrite=True)
            RNNSpyModel.capture_index += 1
            return 0

        features = input_dict["obs"]
        cell_size = 3
        last_layer = add_time_dimension(features, self.seq_lens)

        # Setup the LSTM cell
        lstm = rnn.BasicLSTMCell(cell_size, state_is_tuple=True)
        self.state_init = [
            np.zeros(lstm.state_size.c, np.float32),
            np.zeros(lstm.state_size.h, np.float32)
        ]

        # Setup LSTM inputs
        if self.state_in:
            c_in, h_in = self.state_in
        else:
            c_in = tf.placeholder(
                tf.float32, [None, lstm.state_size.c], name="c")
            h_in = tf.placeholder(
                tf.float32, [None, lstm.state_size.h], name="h")
        self.state_in = [c_in, h_in]

        # Setup LSTM outputs
        state_in = rnn.LSTMStateTuple(c_in, h_in)
        lstm_out, lstm_state = tf.nn.dynamic_rnn(
            lstm,
            last_layer,
            initial_state=state_in,
            sequence_length=self.seq_lens,
            time_major=False,
            dtype=tf.float32)

        self.state_out = list(lstm_state)
        spy_fn = tf.py_func(
            spy, [
                last_layer,
                self.state_in,
                self.state_out,
                self.seq_lens,
            ],
            tf.int64,
            stateful=True)

        # Compute outputs
        with tf.control_dependencies([spy_fn]):
            last_layer = tf.reshape(lstm_out, [-1, cell_size])
            logits = linear(last_layer, num_outputs, "action",
                            normc_initializer(0.01))
        return logits, last_layer


class DebugCounterEnv(gym.Env):
    def __init__(self):
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(0, 100, (1, ))
        self.i = 0

    def reset(self):
        self.i = 0
        return [self.i]

    def step(self, action):
        self.i += 1
        return [self.i], self.i % 3, self.i >= 15, {}


class RNNSequencing(unittest.TestCase):
    def testRNNSequences(self):
        ModelCatalog.register_custom_model("rnn", RNNSpyModel)
        register_env("counter", lambda _: DebugCounterEnv())
        pg = PGAgent(
            env="counter",
            config={
                "num_workers": 0,
                "sample_batch_size": 10,
                "train_batch_size": 10,
                "model": {
                    "custom_model": "rnn",
                    "max_seq_len": 4,
                },
            })
        pg.train()
        pg.train()

        batch0 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_0"))
        self.assertEqual(
            batch0["sequences"].tolist(),
            [[[0], [1], [2], [3]], [[4], [5], [6], [7]], [[8], [9], [0], [0]]])
        self.assertEqual(batch0["seq_lens"].tolist(), [4, 4, 2])
        self.assertEqual(batch0["state_in"][0][0].tolist(), [0, 0, 0])
        self.assertEqual(batch0["state_in"][1][0].tolist(), [0, 0, 0])
        self.assertGreater(abs(np.sum(batch0["state_in"][0][1])), 0)
        self.assertGreater(abs(np.sum(batch0["state_in"][1][1])), 0)
        self.assertTrue(
            np.allclose(batch0["state_in"][0].tolist()[1:],
                        batch0["state_out"][0].tolist()[:-1]))
        self.assertTrue(
            np.allclose(batch0["state_in"][1].tolist()[1:],
                        batch0["state_out"][1].tolist()[:-1]))

        batch1 = pickle.loads(
            ray.experimental.internal_kv._internal_kv_get("rnn_spy_in_1"))
        print(batch1)
        self.assertEqual(batch1["sequences"].tolist(), [
            [[10], [11], [12], [13]],
            [[14], [0], [0], [0]],
            [[0], [1], [2], [3]],
            [[4], [0], [0], [0]],
        ])
        self.assertEqual(batch1["seq_lens"].tolist(), [4, 1, 4, 1])
        self.assertEqual(batch1["state_in"][0][2].tolist(), [0, 0, 0])
        self.assertEqual(batch1["state_in"][1][2].tolist(), [0, 0, 0])
        self.assertGreater(abs(np.sum(batch1["state_in"][0][0])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][1][0])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][0][1])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][1][1])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][0][3])), 0)
        self.assertGreater(abs(np.sum(batch1["state_in"][1][3])), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
