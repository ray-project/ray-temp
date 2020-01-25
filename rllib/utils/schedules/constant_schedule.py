from ray.rllib.utils.schedules.schedule import Schedule


class ConstantSchedule(Schedule):
    """
    A Schedule where the value remains constant over time.
    """
    def __init__(self, value):
        """
        Args:
            value (float): The constant value to return, independently of time.
        """
        super().__init__()
        self._v = value

    def value(self, t=None):
        return self._v
