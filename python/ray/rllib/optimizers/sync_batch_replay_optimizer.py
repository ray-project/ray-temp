from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.evaluation.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.timer import TimerStat


class SyncBatchReplayOptimizer(PolicyOptimizer):
    """Variant of the sync replay optimizer that replays entire batches.

    Does not currently support prioritization."""

    @override(PolicyOptimizer)
    def _init(self, learning_starts=1000, buffer_size=10000):
        self.replay_starts = learning_starts
        self.max_buffer_size = buffer_size
        assert self.max_buffer_size >= self.replay_starts

        # List of buffered sample batches
        self.replay_buffer = []
        self.buffer_size = 0

        # Stats
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.learner_stats = {}

    @override(PolicyOptimizer)
    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.remote_evaluators:
                batches = ray.get(
                    [e.sample.remote() for e in self.remote_evaluators])
            else:
                batches = [self.local_evaluator.sample()]

            # Handle everything as if multiagent
            tmp = []
            for batch in batches:
                if isinstance(batch, SampleBatch):
                    batch = MultiAgentBatch({
                        DEFAULT_POLICY_ID: batch
                    }, batch.count)
                tmp.append(batch)
            batches = tmp

            for batch in batches:
                self.replay_buffer.append(batch)
                self.num_steps_sampled += batch.count
                self.buffer_size += batch.count
                while self.buffer_size > self.max_buffer_size:
                    evicted = self.replay_buffer.pop(0)
                    self.buffer_size -= evicted.count

        if self.num_steps_sampled >= self.replay_starts:
            self._optimize()

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
                "opt_peak_throughput": round(self.grad_timer.mean_throughput,
                                             3),
                "opt_samples": round(self.grad_timer.mean_units_processed, 3),
                "learner": self.learner_stats,
            })

    def _optimize(self):
        samples = random.choice(self.replay_buffer)
        with self.grad_timer:
            info_dict = self.local_evaluator.compute_apply(samples)
            for policy_id, info in info_dict.items():
                if "stats" in info:
                    self.learner_stats[policy_id] = info["stats"]
            self.grad_timer.push_units_processed(samples.count)
        self.num_steps_trained += samples.count
