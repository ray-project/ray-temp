from abc import ABCMeta, abstractmethod

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


@DeveloperAPI
class Schedule(metaclass=ABCMeta):
    """Schedule classes implement various time-dependent scheduling schemas.

    - Constant behavior.
    - Linear decay.
    - Piecewise decay.
    - Exponential decay.

    Useful for backend-agnostic rate/weight changes for learning rates,
    exploration epsilons, beta parameters for prioritized replay, loss weights
    decay, etc..

    Each schedule can be called directly with the `t` (absolute time step)
    value and returns the value dependent on the Schedule and the passed time.
    """

    def __init__(self, framework):
        self.framework = framework

    def value(self, t):
        """Generates the value given a timestep (based on schedule's logic).

        Args:
            t (int): The time step. This could be a tf.Tensor.

        Returns:
            any: The calculated value depending on the schedule and `t`.
        """
        if self.framework in ["tf2", "tf", "tfe"]:
            return self._tf_value_op(t)
        return self._value(t)

    def __call__(self, t):
        """Simply calls self.value(t). Implemented to make Schedules callable.
        """
        return self.value(t)

    @DeveloperAPI
    @abstractmethod
    def _value(self, t):
        """
        Returns the value based on a time step input.

        Args:
            t (int): The time step. This could be a tf.Tensor.

        Returns:
            any: The calculated value depending on the schedule and `t`.
        """
        raise NotImplementedError

    @DeveloperAPI
    def _tf_value_op(self, t):
        """
        Returns the tf-op that calculates the value based on a time step input.

        Args:
            t (tf.Tensor): The time step op (int tf.Tensor).

        Returns:
            tf.Tensor: The calculated value depending on the schedule and `t`.
        """
        # By default (most of the time), tf should work with python code.
        # Override only if necessary.
        return self._value(t)
