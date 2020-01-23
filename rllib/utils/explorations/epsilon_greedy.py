import gym
import numpy as np
import random

from ray.rllib.utils.explorations.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.annotations import override

tf = try_import_tf()


class EpsilonGreedy(Exploration):
    """
    An epsilon-greedy Exploration class that produces exploration actions
    when given a Model's output and a current epsilon value (based on some
    Schedule).
    """
    def __init__(
            self,
            action_space,
            initial_epsilon=1.0,
            final_epsilon=0.1,
            schedule_max_timesteps=1e6,
            exploration_fraction=0.1,
            framework=None
    ):
        """
        Args:
            action_space (Space): The gym action space used by the environment.
            initial_epsilon (float): The initial epsilon value to use.
            final_epsilon (float): The final epsilon value to use.
            schedule_max_timesteps (int): How many timesteps the Schedule
                should decay over.
            exploration_fraction (float): How many .
            framework (Optional[str]): One of None, "tf", "torch".
        """
        # For now, require Discrete action space (may loosen this restriction
        # in the future).
        assert isinstance(action_space, gym.spaces.Discrete)
        super(EpsilonGreedy, self).__init__(framework=framework)

        self.action_space = action_space
        # Create a framework-specific Schedule object.
        self.epsilon_schedule = Schedule.from_config(
            initial_p=initial_epsilon, final_p=final_epsilon,
            max_timesteps=schedule_max_timesteps,
            end_t_pct=exploration_fraction, framework=framework
        )
        # The latest (current) time_step value received.
        self.last_time_step = 0

    @override(Exploration)
    def get_action(self, model_output, model, action_dist, time_step):
        if self.framework == "tf":
            return self._get_tf_action_op(model_output, time_step)

        self.last_time_step = time_step
        # Get the current epsilon.
        epsilon = self.epsilon_schedule.value(time_step)

        # "Epsilon-case": Return a random action.
        if random.random() < epsilon:
            return np.random.randint(0, model_output.shape[1], size=[
                model_output.shape[0]
            ]), np.ones(model_output.shape[0])
        # Return the greedy (argmax) action.
        else:
            return np.argmax(model_output, axis=1), \
                   np.ones(model_output.shape[0])

    def _get_tf_action_op(self, model_output, time_step):
        """
        Tf helper method to produce the tf op for an epsilon exploration
            action.

        Args:
            model_output (any): The Model's output Tensor(s).
            time_step (int): The current (sampling) time step.

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        epsilon = self.epsilon_schedule.value(time_step)
        cond =  tf.cond(
            condition=tf.random_uniform() < epsilon,
            true_fn=lambda: tf.random_uniform(
                shape=tf.shape(model_output), maxval=model_output.shape[1],
                dtype=tf.int32
            ),
            false_fn=lambda: tf.argmax(model_output, axis=1),
        ), tf.ones(model_output.shape[0])

        # Update `last_time_step` and return action op.
        update_op = tf.compat.v1.assign_add(self.last_time_step, 1)
        with tf.control_dependencies([update_op]):
            return cond

    @override(Exploration)
    def get_state(self):
        return self.last_time_step

    @override(Exploration)
    def set_state(self, exploration_state):
        self.last_time_step.set(exploration_state)

    @override(Exploration)
    def reset_state(self):
        self.last_time_step.set(0)

    @override(Exploration)
    def merge_states(self, exploration_states):
        self.last_time_step.set(np.reduce_mean(exploration_states))
