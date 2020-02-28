from typing import Union

from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration, TensorType
from ray.rllib.utils.framework import try_import_tf, try_import_torch, \
    get_variable
from ray.rllib.utils.schedules import PiecewiseSchedule
from ray.rllib.models.modelv2 import ModelV2

tf = try_import_tf()
torch, _ = try_import_torch()


class EpsilonGreedy(Exploration):
    """Epsilon-greedy Exploration class that produces exploration actions.

    When given a Model's output and a current epsilon value (based on some
    Schedule), it produces a random action (if rand(1) < eps) or
    uses the model-computed one (if rand(1) >= eps).
    """

    def __init__(self,
                 action_space,
                 initial_epsilon=1.0,
                 final_epsilon=0.05,
                 epsilon_timesteps=int(1e5),
                 num_workers=None,
                 worker_index=None,
                 epsilon_schedule=None,
                 framework="tf"):
        """Create an EpsilonGreedy exploration class.

        Args:
            action_space (Space): The gym action space used by the environment.
            initial_epsilon (float): The initial epsilon value to use.
            final_epsilon (float): The final epsilon value to use.
            epsilon_timesteps (int): The time step after which epsilon should
                always be `final_epsilon`.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            epsilon_schedule (Optional[Schedule]): An optional Schedule object
                to use (instead of constructing one from the given parameters).
            framework (Optional[str]): One of None, "tf", "torch".
        """
        # For now, require Discrete action space (may loosen this restriction
        # in the future).
        assert framework is not None
        super().__init__(
            action_space=action_space,
            num_workers=num_workers,
            worker_index=worker_index,
            framework=framework)

        self.epsilon_schedule = epsilon_schedule or PiecewiseSchedule(
            endpoints=[(0, initial_epsilon),
                       (epsilon_timesteps, final_epsilon)],
            outside_value=final_epsilon,
            framework=self.framework)

        # The current timestep value (tf-var or python int).
        self.last_timestep = get_variable(
            0, framework=framework, tf_name="timestep")

    @override(Exploration)
    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):

        if self.framework == "tf":
            return self._get_tf_exploration_action_op(distribution_inputs,
                                                      explore, timestep)
        else:
            return self._get_torch_exploration_action(distribution_inputs,
                                                      explore, timestep)

    def _get_tf_exploration_action_op(self, q_values, explore, timestep):
        """TF method to produce the tf op for an epsilon exploration action.

        Args:
            q_values (Tensor): The Q-values coming from some q-model.

        Returns:
            tf.Tensor: The tf exploration-action op.
        """
        epsilon = tf.convert_to_tensor(self.epsilon_schedule(timestep))

        # Get the exploit action as the one with the highest logit value.
        exploit_action = tf.argmax(q_values, axis=1)

        batch_size = tf.shape(q_values)[0]
        # Mask out actions with q-value=-inf so that we don't
        # even consider them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(q_values, tf.float32.min),
            tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
        random_actions = tf.squeeze(
            tf.multinomial(random_valid_action_logits, 1), axis=1)

        chose_random = tf.random_uniform(
            tf.stack([batch_size]),
            minval=0, maxval=1, dtype=epsilon.dtype) \
            < epsilon

        action = tf.cond(
            pred=tf.constant(explore, dtype=tf.bool)
            if isinstance(explore, bool) else explore,
            true_fn=(
                lambda: tf.where(chose_random, random_actions, exploit_action)
            ),
            false_fn=lambda: exploit_action)

        assign_op = tf.assign(self.last_timestep, timestep)
        with tf.control_dependencies([assign_op]):
            return action, tf.zeros_like(action, dtype=tf.float32)

    def _get_torch_exploration_action(self, q_values, explore, timestep):
        """Torch method to produce an epsilon exploration action.

        Args:
            q_values (Tensor): The Q-values coming from some q-model.

        Returns:
            torch.Tensor: The exploration-action.
        """
        # Set last time step or (if not given) increase by one.
        self.last_timestep = timestep
        _, exploit_action = torch.max(q_values, 1)
        action_logp = torch.zeros_like(exploit_action)

        # Explore.
        if explore:
            # Get the current epsilon.
            epsilon = self.epsilon_schedule(self.last_timestep)
            batch_size = q_values.size()[0]
            # Mask out actions, whose Q-values are -inf, so that we don't
            # even consider them for exploration.
            random_valid_action_logits = torch.where(
                q_values == float("-inf"),
                torch.ones_like(q_values) * float("-inf"),
                torch.ones_like(q_values))
            # A random action.
            random_actions = torch.squeeze(
                torch.multinomial(random_valid_action_logits, 1), axis=1)
            # Pick either random or greedy.
            action = torch.where(
                torch.empty((batch_size, )).uniform_() < epsilon,
                random_actions, exploit_action)

            return action, action_logp
        # Return the deterministic "sample" (argmax) over the logits.
        else:
            return exploit_action, action_logp

    @override(Exploration)
    def get_info(self):
        eps = self.epsilon_schedule(self.last_timestep)
        return {"cur_epsilon": eps}
