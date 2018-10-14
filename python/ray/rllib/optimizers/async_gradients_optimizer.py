from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.timer import TimerStat

from ray.rllib.utils.timer import setup_custom_logger
import time
# _LOGGER = setup_custom_logger(__name__)

class AsyncGradientsOptimizer(PolicyOptimizer):
    """An asynchronous RL optimizer, e.g. for implementing A3C.

    This optimizer asynchronously pulls and applies gradients from remote
    evaluators, sending updated weights back as needed. This pipelines the
    gradient computations on the remote workers.
    """

    def _init(self, grads_per_step=100):
        self.apply_timer = TimerStat()
        self.wait_timer = TimerStat()
        self.dispatch_timer = TimerStat()
        self.grads_per_step = grads_per_step
        self.learner_stats = {}
        if not self.remote_evaluators:
            raise ValueError(
                "Async optimizer requires at least 1 remote evaluator")

    def step(self):
        weights = ray.put(self.local_evaluator.get_weights())
        pending_gradients = {}
        num_gradients = 0

        # Kick off the first wave of async tasks
        for e in self.remote_evaluators:
            e.set_weights.remote(weights)
            future = e.compute_gradients.remote(e.sample.remote())
            pending_gradients[future] = e
            num_gradients += 1

        # Note: can't use wait: https://github.com/ray-project/ray/issues/1128
        while pending_gradients:
            with self.wait_timer:
                start = time.time()
                # _LOGGER.info("Waiting for next ray result")
                wait_results = ray.wait(list(pending_gradients.keys()), num_returns=1)

                ready = wait_results[0]
                future = ready[0]

                gradient, info = ray.get(future)

                e = pending_gradients.pop(future)

                # _LOGGER.info("Got next result: {}".format(future))
                # _LOGGER.info("Time: {}".format(time.time() - start))
                if "stats" in info:
                    self.learner_stats = info["stats"]

            if gradient is not None:
                with self.apply_timer:
                    self.local_evaluator.apply_gradients(gradient)
                self.num_steps_sampled += info["batch_count"]
                self.num_steps_trained += info["batch_count"]

            if num_gradients < self.grads_per_step:
                with self.dispatch_timer:
                    e.set_weights.remote(self.local_evaluator.get_weights())
                    future = e.compute_gradients.remote(e.sample.remote())

                    pending_gradients[future] = e
                    num_gradients += 1

    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "wait_time_ms": round(1000 * self.wait_timer.mean, 3),
                "apply_time_ms": round(1000 * self.apply_timer.mean, 3),
                "dispatch_time_ms": round(1000 * self.dispatch_timer.mean, 3),
                "learner": self.learner_stats,
            })
