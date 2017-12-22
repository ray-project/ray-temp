from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.timer import TimerStat


class LocalSyncOptimizer(Optimizer):
    """A simple synchronous RL optimizer.

    In each step, this optimizer pulls samples from a number of remote
    evaluators, concatenates them, and then updates a local model. The updated
    model weights are then broadcast to all remote evaluators.
    """

    def _init(self):
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()

    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                filters = [
                    ray.put(f) for f in self.local_evaluator.get_filters()]
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)
                    e.sync_filters.remote(*filters)

        with self.sample_timer:
            if self.remote_evaluators:
                fut_samples, fut_infos = zip(
                    *[e.sample.remote() for e in self.remote_evaluators])
                samples = SampleBatch.concat_samples(ray.get(future_samples))
                infos = ray.get(fut_infos)
            else:
                samples = self.local_evaluator.sample()

        with self.grad_timer:
            grad, _ = self.local_evaluator.compute_gradients(samples)
            self.local_evaluator.apply_gradients(grad)
            for info in infos:
                self.local_evaluator.merge_filters(
                    info["obs_filter"], info["rew_filter"])


    def stats(self):
        return {
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3),
        }
