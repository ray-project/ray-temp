from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Optimizer(object):
    """RLlib optimizers encapsulate distributed RL optimization strategies.

    For example, AsyncOptimizer is used for A3C, and LocalMultiGpuOptimizer is
    used for PPO. These optimizers are all pluggable however, it is possible
    to mix as match as needed.

    In order for an algorithm to use an RLlib optimizer, it must implement
    the Evaluator interface and pass a number of remote Evaluators to its
    Optimizer of choice. The Optimizer uses these Evaluators to sample from the
    environment and compute model gradient updates.
    """

    def __init__(self, local_evaluator, remote_evaluators):
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators

    def step(self):
        """Takes a logical optimization step."""

        raise NotImplementedError

    def stats(self):
        """Returns a dictionary of internal performance statistics."""

        return {}
