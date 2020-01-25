from ray.rllib.utils.schedules.schedule import Schedule
from ray.rllib.utils.framework import check_framework


class ExponentialSchedule(Schedule):

    def __init__(self, schedule_timesteps, final_p=0.0, initial_p=1.0,
                 decay_rate=0.1, framework=None):
        """
        Exponential decay schedule from initial_p to final_p over
        schedule_timesteps. After this many time steps always `final_p` is
        returned.

        Agrs:
            schedule_timesteps (int): Number of time steps for which to
                linearly anneal initial_p to final_p
            final_p (float): Final output value.
            initial_p (float): Initial output value.
            decay_rate (float): The percentage of the original value after
                100% of the time has been reached (see formula above).
                >0.0: The smaller the decay-rate, the stronger the decay.
                1.0: No decay at all.
            framework (Optional[str]): One of "tf", "torch", or None.
        """
        super().__init__()
        assert self.schedule_timesteps > 0
        self.schedule_timesteps = schedule_timesteps
        self.final_p = final_p
        self.initial_p = initial_p
        self.decay_rate = decay_rate
        self.framework = check_framework(framework)

    def value(self, t):
        """
        Returns the result of:
        initial_p * decay_rate ** (`t`/t_max)
        """
        return self.initial_p * \
               self.decay_rate ** (t / self.schedule_timesteps)
