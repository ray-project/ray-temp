"""Implements the IMPALA architecture.

https://arxiv.org/abs/1802.01561"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import time
import threading

from six.moves import queue

import ray
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

SAMPLE_QUEUE_DEPTH = 2
LEARNER_QUEUE_MAX_SIZE = 16


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from sample trajectories.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(self, local_evaluator):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_evaluator = local_evaluator
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_MAX_SIZE)
        self.outqueue = queue.Queue()
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.load_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stats = {}

    def run(self):
        while True:
            self.step()

    def step(self):
        with self.queue_timer:
            batch = self.inqueue.get()

        with self.grad_timer:
            fetches = self.local_evaluator.compute_apply(batch)
            self.weights_updated = True
            if "stats" in fetches:
                self.stats = fetches["stats"]

        self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())


class TFMultiGPULearner(LearnerThread):
    def __init__(self,
                 local_evaluator,
                 num_gpus=2,
                 lr=0.0005,
                 train_batch_size=500,
                 replay_batch_slots=0):
        import tensorflow as tf

        LearnerThread.__init__(self, local_evaluator)
        self.lr = lr
        self.train_batch_size = train_batch_size
        if not num_gpus:
            self.devices = ["/cpu:0"]
        else:
            self.devices = ["/gpu:{}".format(i) for i in range(num_gpus)]
            print("TFMultiGPULearner devices", self.devices)
        assert self.train_batch_size % len(self.devices) == 0
        assert self.train_batch_size >= len(self.devices), "batch too small"
        self.per_device_batch_size = int(
            self.train_batch_size / len(self.devices))
        self.policy = self.local_evaluator.policy_map["default"]

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        with self.local_evaluator.tf_sess.graph.as_default():
            with self.local_evaluator.tf_sess.as_default():
                with tf.variable_scope("default", reuse=tf.AUTO_REUSE):
                    if self.policy._state_inputs:
                        rnn_inputs = self.policy._state_inputs + [
                            self.policy._seq_lens
                        ]
                    else:
                        rnn_inputs = []
                    self.par_opt = LocalSyncParallelOptimizer(
                        tf.train.AdamOptimizer(self.lr), self.devices,
                        [v for _, v in self.policy.loss_inputs()], rnn_inputs,
                        self.per_device_batch_size, self.policy.copy,
                        os.getcwd())

                self.sess = self.local_evaluator.tf_sess
                self.sess.run(tf.global_variables_initializer())

        self.replay_batch_slots = replay_batch_slots
        self.replay_buffer = []

    def step(self):
        with self.queue_timer:
            if (self.inqueue.empty() and self.replay_batch_slots > 0
                    and self.replay_buffer):
                batch = random.choice(self.replay_buffer)
            else:
                batch = self.inqueue.get()
                if self.replay_batch_slots > 0:
                    self.replay_buffer.append(batch)
                    if len(self.replay_buffer) > self.replay_batch_slots:
                        self.replay_buffer.pop(0)
            assert batch.count == self.train_batch_size

        with self.load_timer:
            tuples = self.policy._get_loss_inputs_dict(batch)
            data_keys = [ph for _, ph in self.policy.loss_inputs()]
            if self.policy._state_inputs:
                state_keys = (
                    self.policy._state_inputs + [self.policy._seq_lens])
            else:
                state_keys = []
            tuples_per_device = self.par_opt.load_data(
                self.sess, [tuples[k] for k in data_keys],
                [tuples[k] for k in state_keys])
            assert int(tuples_per_device) == int(self.per_device_batch_size)

        with self.grad_timer:
            fetches = self.par_opt.optimize(self.sess, 0)
            self.weights_updated = True
            if "stats" in fetches:
                self.stats = fetches["stats"]

        self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())


class AsyncSamplesOptimizer(PolicyOptimizer):
    """Main event loop of the IMPALA architecture.

    This class coordinates the data transfers between the learner thread
    and remote evaluators (IMPALA actors).
    """

    def _init(self,
              train_batch_size=500,
              sample_batch_size=50,
              num_gpus=0,
              lr=0.0005,
              debug=False,
              replay_batch_slots=0):
        self.debug = debug
        self.learning_started = False
        self.train_batch_size = train_batch_size

        if num_gpus > 1:
            if train_batch_size // num_gpus % sample_batch_size != 0:
                raise ValueError(
                    "Sample batches must evenly divide across GPUs.")
            self.learner = TFMultiGPULearner(
                self.local_evaluator,
                lr=lr,
                num_gpus=num_gpus,
                train_batch_size=train_batch_size,
                replay_batch_slots=replay_batch_slots)
        else:
            self.learner = LearnerThread(self.local_evaluator)
        self.learner.start()

        assert len(self.remote_evaluators) > 0

        # Stats
        self.timers = {
            k: TimerStat()
            for k in
            ["put_weights", "enqueue", "sample_processing", "train", "sample"]
        }
        self.num_weight_syncs = 0
        self.learning_started = False

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            for _ in range(SAMPLE_QUEUE_DEPTH):
                self.sample_tasks.add(ev, ev.sample.remote())

        self.batch_buffer = []

    def step(self):
        assert self.learner.is_alive()
        start = time.time()
        sample_timesteps, train_timesteps = self._step()
        time_delta = time.time() - start
        self.timers["sample"].push(time_delta)
        self.timers["sample"].push_units_processed(sample_timesteps)
        if train_timesteps > 0:
            self.learning_started = True
        if self.learning_started:
            self.timers["train"].push(time_delta)
            self.timers["train"].push_units_processed(train_timesteps)
        self.num_steps_sampled += sample_timesteps
        self.num_steps_trained += train_timesteps

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        weights = None

        with self.timers["sample_processing"]:
            for ev, sample_batch in self.sample_tasks.completed_prefetch():
                sample_batch = ray.get(sample_batch)
                sample_timesteps += sample_batch.count
                self.batch_buffer.append(sample_batch)
                if sum(b.count
                       for b in self.batch_buffer) >= self.train_batch_size:
                    train_batch = self.batch_buffer[0].concat_samples(
                        self.batch_buffer)
                    with self.timers["enqueue"]:
                        self.learner.inqueue.put(train_batch)
                    self.batch_buffer = []

                # Note that it's important to pull new weights once
                # updated to avoid excessive correlation between actors
                if weights is None or self.learner.weights_updated:
                    self.learner.weights_updated = False
                    with self.timers["put_weights"]:
                        weights = ray.put(self.local_evaluator.get_weights())
                ev.set_weights.remote(weights)
                self.num_weight_syncs += 1

                # Kick off another sample request
                self.sample_tasks.add(ev, ev.sample.remote())

        while not self.learner.outqueue.empty():
            count = self.learner.outqueue.get()
            train_timesteps += count

        return sample_timesteps, train_timesteps

    def stats(self):
        timing = {
            "{}_time_ms".format(k): round(1000 * self.timers[k].mean, 3)
            for k in self.timers
        }
        timing["learner_grad_time_ms"] = round(
            1000 * self.learner.grad_timer.mean, 3)
        timing["learner_load_time_ms"] = round(
            1000 * self.learner.load_timer.mean, 3)
        timing["learner_dequeue_time_ms"] = round(
            1000 * self.learner.queue_timer.mean, 3)
        stats = {
            "sample_throughput": round(self.timers["sample"].mean_throughput,
                                       3),
            "train_throughput": round(self.timers["train"].mean_throughput, 3),
            "num_weight_syncs": self.num_weight_syncs,
        }
        debug_stats = {
            "timing_breakdown": timing,
            "pending_sample_tasks": self.sample_tasks.count,
            "learner_queue": self.learner.learner_queue_size.stats(),
        }
        if self.debug:
            stats.update(debug_stats)
        if self.learner.stats:
            stats["learner"] = self.learner.stats
        return dict(PolicyOptimizer.stats(self), **stats)
