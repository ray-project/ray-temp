from gym.spaces import Discrete
import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.framework import get_variable
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.numpy import softmax, SMALL_NUMBER

tf = try_import_tf()
torch, _ = try_import_torch()


class ParameterNoise(Exploration):
    """An exploration that changes a Model's parameters.

    Implemented based on:
    [1] https://blog.openai.com/better-exploration-with-parameter-noise/
    [2] https://arxiv.org/pdf/1706.01905.pdf

    At the beginning of an episode, Gaussian noise is added to all weights
    of the model. At the end of the episode, the noise is undone and an action
    diff (pi-delta) is calculated, from which we determine the changes in the
    noise's stddev for the next episode.
    """

    def __init__(self,
                 action_space,
                 *,
                 framework: str,
                 policy_config: dict,
                 model: ModelV2,
                 initial_stddev=1.0,
                 random_timesteps=10000,
                 sub_exploration=None,
                 **kwargs):
        """Initializes a ParameterNoise Exploration object.

        Args:
            initial_stddev (float): The initial stddev to use for the noise.
            random_timesteps (int): The number of timesteps to act completely
                randomly (see [1]).
            sub_exploration (Optional[dict]): Optional sub-exploration config.
                None for auto-detection/setup.
        """
        assert framework is not None
        super().__init__(
            action_space,
            policy_config=policy_config,
            model=model,
            framework=framework,
            **kwargs)

        self.stddev = get_variable(
            initial_stddev, framework=self.framework, tf_name="stddev")
        self.stddev_val = initial_stddev  # Out-of-graph tf value holder.

        # The weight variables of the Model where noise should be applied to.
        # This excludes any variable, whose name contains "LayerNorm" (those
        # are BatchNormalization layers, which should not be perturbed).
        self.model_variables = [
            v for k, v in self.model.variables(as_dict=True).items()
            if "LayerNorm" not in k
        ]
        # Our noise to be added to the weights. Each item in `self.noise`
        # corresponds to one Model variable and holding the Gaussian noise to
        # be added to that variable (weight).
        self.noise = []
        for var in self.model_variables:
            name_ = var.name.split(":")[0] + "_noisy" if var.name else ""
            self.noise.append(
                get_variable(
                    np.zeros(var.shape, dtype=np.float32),
                    framework=self.framework,
                    tf_name=name_,
                    torch_tensor=True))

        # tf-specific ops to sample, assign and remove noise.
        if self.framework == "tf" and not tf.executing_eagerly():
            self.tf_sample_new_noise_op = \
                self._tf_sample_new_noise_op()
            self.tf_add_stored_noise_op = \
                self._tf_add_stored_noise_op()
            self.tf_remove_noise_op = \
                self._tf_remove_noise_op()
            # Create convenience sample+add op for tf.
            with tf.control_dependencies([self.tf_sample_new_noise_op]):
                add_op = self._tf_add_stored_noise_op()
            with tf.control_dependencies([add_op]):
                self.tf_sample_new_noise_and_add_op = tf.no_op()

        # Whether the Model's weights currently have noise added or not.
        self.weights_are_currently_noisy = False

        # Auto-detection of underlying exploration functionality.
        if sub_exploration is None:
            # For discrete action spaces, use an underlying EpsilonGreedy with
            # a special schedule.
            if isinstance(self.action_space, Discrete):
                sub_exploration = {
                    "type": "EpsilonGreedy",
                    "epsilon_schedule": {
                        "type": "PiecewiseSchedule",
                        # Step function (see [2]).
                        "endpoints": [(0, 1.0), (random_timesteps + 1, 1.0),
                                      (random_timesteps + 2, 0.01)],
                        "outside_value": 0.01
                    }
                }
            # TODO(sven): Implement for any action space.
            else:
                raise NotImplementedError

        self.sub_exploration = from_config(
            Exploration,
            sub_exploration,
            framework=self.framework,
            action_space=self.action_space,
            policy_config=self.policy_config,
            model=self.model,
            **kwargs)

        # Whether we need to call `self._delayed_on_episode_start` before
        # the forward pass.
        self.episode_started = False

    @override(Exploration)
    def before_compute_actions(self,
                               *,
                               timestep=None,
                               explore=None,
                               tf_sess=None):
        explore = explore if explore is not None else \
            self.policy_config["explore"]

        # Is this the first forward pass in the new episode? If yes, do the
        # noise re-sampling and add to weights.
        if self.episode_started:
            self._delayed_on_episode_start(explore, tf_sess)

        # Add noise if necessary.
        if explore and not self.weights_are_currently_noisy:
            self._add_stored_noise(tf_sess=tf_sess)
        # Remove noise if necessary.
        elif not explore and self.weights_are_currently_noisy:
            self._remove_noise(tf_sess=tf_sess)

    @override(Exploration)
    def get_exploration_action(self,
                               *,
                               action_distribution,
                               timestep,
                               explore=True):
        # Use our sub-exploration object to handle the final exploration
        # action (depends on the algo-type/action-space/etc..).
        return self.sub_exploration.get_exploration_action(
            action_distribution=action_distribution,
            timestep=timestep,
            explore=explore)

    @override(Exploration)
    def on_episode_start(self,
                         policy,
                         *,
                         environment=None,
                         episode=None,
                         tf_sess=None):
        # We have to delay the noise-adding step by one forward call.
        # This is due to the fact that the optimizer does it's step right
        # after the episode was reset (and hence the noise was already added!).
        # We don't want to update into a noisy net.
        self.episode_started = True

    def _delayed_on_episode_start(self, explore, tf_sess):
        # Sample fresh noise and add to weights.
        if explore:
            self._sample_new_noise_and_add(tf_sess=tf_sess, override=True)
        # Only sample, don't apply anything to the weights.
        else:
            self._sample_new_noise(tf_sess=tf_sess)
        self.episode_started = False

    @override(Exploration)
    def on_episode_end(self,
                       policy,
                       *,
                       environment=None,
                       episode=None,
                       tf_sess=None):
        # Remove stored noise from weights (only if currently noisy).
        if self.weights_are_currently_noisy:
            self._remove_noise(tf_sess=tf_sess)

    @override(Exploration)
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        noisy_action_dist = noise_free_action_dist = None
        # Adjust the stddev depending on the action (pi)-distance.
        # Also see [1] for details.
        _, _, fetches = policy.compute_actions(
            obs_batch=sample_batch[SampleBatch.CUR_OBS],
            # TODO(sven): What about state-ins and seq-lens?
            prev_action_batch=sample_batch.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=sample_batch.get(SampleBatch.PREV_REWARDS),
            explore=self.weights_are_currently_noisy)

        # Categorical case (e.g. DQN).
        if policy.dist_class in (Categorical, TorchCategorical):
            action_dist = softmax(fetches[SampleBatch.ACTION_DIST_INPUTS])
        else:  # TODO(sven): Other action-dist cases.
            raise NotImplementedError

        if self.weights_are_currently_noisy:
            noisy_action_dist = action_dist
        else:
            noise_free_action_dist = action_dist

        _, _, fetches = policy.compute_actions(
            obs_batch=sample_batch[SampleBatch.CUR_OBS],
            # TODO(sven): What about state-ins and seq-lens?
            prev_action_batch=sample_batch.get(SampleBatch.PREV_ACTIONS),
            prev_reward_batch=sample_batch.get(SampleBatch.PREV_REWARDS),
            explore=not self.weights_are_currently_noisy)

        # Categorical case (e.g. DQN).
        if policy.dist_class in (Categorical, TorchCategorical):
            action_dist = softmax(fetches[SampleBatch.ACTION_DIST_INPUTS])

        if noisy_action_dist is None:
            noisy_action_dist = action_dist
        else:
            noise_free_action_dist = action_dist

        # Categorical case (e.g. DQN).
        if policy.dist_class in (Categorical, TorchCategorical):
            # Calculate KL-divergence (DKL(clean||noisy)) according to [2].
            # TODO(sven): Allow KL-divergence to be calculated by our
            #  Distribution classes (don't support off-graph/numpy yet).
            kl_divergence = np.nanmean(
                np.sum(
                    noise_free_action_dist *
                    np.log(noise_free_action_dist /
                           (noisy_action_dist + SMALL_NUMBER)), 1))
            current_epsilon = self.sub_exploration.get_info()["cur_epsilon"]
            if tf_sess is not None:
                current_epsilon = tf_sess.run(current_epsilon)
            delta = -np.log(1 - current_epsilon +
                            current_epsilon / self.action_space.n)
            if kl_divergence <= delta:
                self.stddev_val *= 1.01
            else:
                self.stddev_val /= 1.01

        # Set self.stddev to calculated value.
        if self.framework == "tf":
            self.stddev.load(self.stddev_val, session=tf_sess)
        else:
            self.stddev = self.stddev_val

        return sample_batch

    def _sample_new_noise(self, *, tf_sess=None):
        """Samples new noise and stores it in `self.noise`."""
        if self.framework == "tf":
            if tf.executing_eagerly():
                self._tf_sample_new_noise_op()
            else:
                tf_sess.run(self.tf_sample_new_noise_op)
        else:
            for i in range(len(self.noise)):
                self.noise[i] = torch.normal(
                    mean=torch.zeros(self.noise[i].size()), std=self.stddev)

    def _tf_sample_new_noise_op(self):
        added_noises = []
        for noise in self.noise:
            added_noises.append(
                tf.assign(
                    noise,
                    tf.random_normal(
                        shape=noise.shape,
                        stddev=self.stddev,
                        dtype=tf.float32)))
        return tf.group(*added_noises)

    def _sample_new_noise_and_add(self, *, tf_sess=None, override=False):
        if self.framework == "tf" and not tf.executing_eagerly():
            if override and self.weights_are_currently_noisy:
                tf_sess.run(self.tf_remove_noise_op)
            tf_sess.run(self.tf_sample_new_noise_and_add_op)
        else:
            if override and self.weights_are_currently_noisy:
                self._remove_noise()
            self._sample_new_noise()
            self._add_stored_noise()

        self.weights_are_currently_noisy = True

    def _add_stored_noise(self, *, tf_sess=None):
        """Adds the stored `self.noise` to the model's parameters.

        Note: No new sampling of noise here.

        Args:
            tf_sess (Optional[tf.Session]): The tf-session to use to add the
                stored noise to the (currently noise-free) weights.
            override (bool): If True, undo any currently applied noise first,
                then add the currently stored noise.
        """
        # Make sure we only add noise to currently noise-free weights.
        assert self.weights_are_currently_noisy is False

        if self.framework == "tf":
            if tf.executing_eagerly():
                self._tf_add_stored_noise_op()
            else:
                tf_sess.run(self.tf_add_stored_noise_op)
        # Add stored noise to the model's parameters.
        else:
            for i in range(len(self.noise)):
                # Add noise to weights in-place.
                self.model_variables[i].add_(self.noise[i])

        self.weights_are_currently_noisy = True

    def _tf_add_stored_noise_op(self):
        """Generates tf-op that assigns the stored noise to weights.

        Also used by tf-eager.

        Returns:
            tf.op: The tf op to apply the already stored noise to the NN.
        """
        add_noise_ops = list()
        for var, noise in zip(self.model_variables, self.noise):
            add_noise_ops.append(tf.assign_add(var, noise))
        ret = tf.group(*tuple(add_noise_ops))
        with tf.control_dependencies([ret]):
            return tf.no_op()

    def _remove_noise(self, *, tf_sess=None):
        """
        Removes the current action noise from the model parameters.

        Args:
            tf_sess (Optional[tf.Session]): The tf-session to use to remove
                the noise from the (currently noisy) weights.
        """
        # Make sure we only remove noise iff currently noisy.
        assert self.weights_are_currently_noisy is True

        if self.framework == "tf":
            if tf.executing_eagerly():
                self._tf_remove_noise_op()
            else:
                tf_sess.run(self.tf_remove_noise_op)
        else:
            # Removes the stored noise from the model's parameters.
            for var, noise in zip(self.model_variables, self.noise):
                # Remove noise from weights in-place.
                var.add_(-noise)

        self.weights_are_currently_noisy = False

    def _tf_remove_noise_op(self):
        """Generates a tf-op for removing noise from the model's weights.

        Also used by tf-eager.

        Returns:
            tf.op: The tf op to remve the currently stored noise from the NN.
        """
        remove_noise_ops = list()
        for var, noise in zip(self.model_variables, self.noise):
            remove_noise_ops.append(tf.assign_add(var, -noise))
        ret = tf.group(*tuple(remove_noise_ops))
        with tf.control_dependencies([ret]):
            return tf.no_op()

    @override(Exploration)
    def get_info(self):
        return {"cur_stddev": self.stddev}
