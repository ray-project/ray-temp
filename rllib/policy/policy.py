from abc import ABCMeta, abstractmethod
from collections import namedtuple
import gym
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.components.explorations.exploration import Exploration

# By convention, metrics from optimizing the loss can be reported in the
# `grad_info` dict returned by learn_on_batch() / compute_grads() via this key.
LEARNER_STATS_KEY = "learner_stats"

ACTION_PROB = "action_prob"
ACTION_LOGP = "action_logp"


class TupleActions(namedtuple("TupleActions", ["batches"])):
    """Used to return tuple actions as a list of batches per tuple element."""

    def __new__(cls, batches):
        return super(TupleActions, cls).__new__(cls, batches)

    def numpy(self):
        return TupleActions([b.numpy() for b in self.batches])


@DeveloperAPI
class Policy(metaclass=ABCMeta):
    """An agent policy and loss, i.e., a TFPolicy or other subclass.

    This object defines how to act in the environment, and also losses used to
    improve the policy based on its experiences. Note that both policy and
    loss are defined together for convenience, though the policy itself is
    logically separate.

    All policies can directly extend Policy, however TensorFlow users may
    find TFPolicy simpler to implement. TFPolicy also enables RLlib
    to apply TensorFlow-specific optimizations such as fusing multiple policy
    graphs and multi-GPU support.

    Attributes:
        observation_space (gym.Space): Observation space of the policy.
        action_space (gym.Space): Action space of the policy.
    """

    @DeveloperAPI
    def __init__(self, observation_space, action_space, config):
        """Initialize the graph.

        This is the standard constructor for policies. The policy
        class you pass into RolloutWorker will be constructed with
        these arguments.

        Args:
            observation_space (gym.Space): Observation space of the policy.
            action_space (gym.Space): Action space of the policy.
            config (dict): Policy-specific configuration data.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.config = config

        # Create the Exploration object to use for this Policy.
        self.exploration = Exploration.from_config(config.get("exploration"))
        # The default sampling behavior for actions if not explicitly given
        # in calls to `compute_actions`.
        self.deterministic = config.get("deterministic", False)

    @abstractmethod
    @DeveloperAPI
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        deterministic=None,
                        explore=True,
                        time_step=None,
                        **kwargs):
        """Computes actions for the current policy.

        Args:
            obs_batch (Union[List,np.ndarray]): Batch of observations.
            state_batches (Optional[list]): List of RNN state input batches,
                if any.
            prev_action_batch (Optional[List,np.ndarray]): Batch of previous
                action values.
            prev_reward_batch (Optional[List,np.ndarray]): Batch of previous
                rewards.
            info_batch (info): Batch of info objects.
            episodes (list): MultiAgentEpisode for each obs in obs_batch.
                This provides access to all of the internal episode state,
                which may be useful for model-based or multiagent algorithms.
            deterministic (Optional[bool]): Whether the action should be
                sampled deterministically or stochastically.
                If None, use this Policy's `self.deterministic` value.
            explore (bool): Whether we should use exploration
                (e.g. when training) or not (for inference/evaluation).
            time_step (int): The current (sampling) time step.
            kwargs: forward compatibility placeholder

        Returns:
            actions (np.ndarray): batch of output actions, with shape like
                [BATCH_SIZE, ACTION_SHAPE].
            state_outs (list): list of RNN state output batches, if any, with
                shape like [STATE_SIZE, BATCH_SIZE].
            info (dict): dictionary of extra feature batches, if any, with
                shape like {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        raise NotImplementedError

    @DeveloperAPI
    def compute_single_action(self,
                              obs,
                              state=None,
                              prev_action=None,
                              prev_reward=None,
                              info=None,
                              episode=None,
                              clip_actions=False,
                              deterministic=False,
                              explore=True,
                              time_step=None,
                              **kwargs):
        """Unbatched version of compute_actions.

        Arguments:
            obs (obj): Single observation.
            state (list): List of RNN state inputs, if any.
            prev_action (obj): Previous action value, if any.
            prev_reward (float): Previous reward, if any.
            info (dict): info object, if any
            episode (MultiAgentEpisode): this provides access to all of the
                internal episode state, which may be useful for model-based or
                multi-agent algorithms.
            clip_actions (bool): should the action be clipped
            deterministic (Optional[bool]): Whether the action should be
                sampled deterministically or stochastically.
                If None, use this Policy's `self.deterministic` value.
            explore (bool): Whether we should use exploration (i.e. when
                training) or not (e.g. for inference/evaluation).
            time_step (int): The current (sampling) time step.
            kwargs: forward compatibility placeholder

        Returns:
            actions (obj): single action
            state_outs (list): list of RNN state outputs, if any
            info (dict): dictionary of extra features, if any
        """
        prev_action_batch = None
        prev_reward_batch = None
        info_batch = None
        episodes = None
        state_batch = None
        if prev_action is not None:
            prev_action_batch = [prev_action]
        if prev_reward is not None:
            prev_reward_batch = [prev_reward]
        if info is not None:
            info_batch = [info]
        if episode is not None:
            episodes = [episode]
        if state is not None:
            state_batch = [[s] for s in state]

        [action], state_out, info = self.compute_actions(
            [obs], state_batch,
            prev_action_batch=prev_action_batch,
            prev_reward_batch=prev_reward_batch,
            info_batch=info_batch,
            episodes=episodes,
            deterministic=deterministic,
            explore=explore,
            time_step=time_step
        )

        if clip_actions:
            action = self.clip_action(action, self.action_space)

        # Return action, internal state(s), infos.
        return action, [s[0] for s in state_out], \
            {k: v[0] for k, v in info.items()}

    @DeveloperAPI
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        """Implements algorithm-specific trajectory postprocessing.

        This will be called on each trajectory fragment computed during policy
        evaluation. Each fragment is guaranteed to be only from one episode.

        Arguments:
            sample_batch (SampleBatch): batch of experiences for the policy,
                which will contain at most one episode trajectory.
            other_agent_batches (dict): In a multi-agent env, this contains a
                mapping of agent ids to (policy, agent_batch) tuples
                containing the policy and experiences of the other agents.
            episode (MultiAgentEpisode): this provides access to all of the
                internal episode state, which may be useful for model-based or
                multi-agent algorithms.

        Returns:
            SampleBatch: Postprocessed sample batch.
        """
        return sample_batch

    @DeveloperAPI
    def learn_on_batch(self, samples):
        """Fused compute gradients and apply gradients call.

        Either this or the combination of compute/apply grads must be
        implemented by subclasses.

        Returns:
            grad_info: dictionary of extra metadata from compute_gradients().

        Examples:
            >>> batch = ev.sample()
            >>> ev.learn_on_batch(samples)
        """

        grads, grad_info = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return grad_info

    @DeveloperAPI
    def compute_gradients(self, postprocessed_batch):
        """Computes gradients against a batch of experiences.

        Either this or learn_on_batch() must be implemented by subclasses.

        Returns:
            grads (list): List of gradient output values
            info (dict): Extra policy-specific values
        """
        raise NotImplementedError

    @DeveloperAPI
    def apply_gradients(self, gradients):
        """Applies previously computed gradients.

        Either this or learn_on_batch() must be implemented by subclasses.
        """
        raise NotImplementedError

    @DeveloperAPI
    def get_weights(self):
        """Returns model weights.

        Returns:
            weights (obj): Serializable copy or view of model weights
        """
        raise NotImplementedError

    @DeveloperAPI
    def set_weights(self, weights):
        """Sets model weights.

        Arguments:
            weights (obj): Serializable copy or view of model weights
        """
        raise NotImplementedError

    @DeveloperAPI
    def num_state_tensors(self):
        """
        Returns:
            int: The number of RNN hidden states kept by this Policy's Model.
        """
        return 0

    @DeveloperAPI
    def get_initial_state(self):
        """Returns initial RNN state for the current policy."""
        return []

    @DeveloperAPI
    def get_state(self):
        """Saves all local state.

        Returns:
            state (obj): Serialized local state.
        """
        return self.get_weights()

    @DeveloperAPI
    def set_state(self, state):
        """Restores all local state.

        Arguments:
            state (obj): Serialized local state.
        """
        self.set_weights(state)

    @DeveloperAPI
    def on_global_var_update(self, global_vars):
        """Called on an update to global vars.

        Arguments:
            global_vars (dict): Global variables broadcast from the driver.
        """
        pass

    @DeveloperAPI
    def export_model(self, export_dir):
        """Export Policy to local directory for serving.

        Arguments:
            export_dir (str): Local writable directory.
        """
        raise NotImplementedError

    @DeveloperAPI
    def export_checkpoint(self, export_dir):
        """Export Policy checkpoint to local directory.

        Argument:
            export_dir (str): Local writable directory.
        """
        raise NotImplementedError

    @staticmethod
    def clip_action(action, space):
        """

    Called to clip actions to the specified range of this policy.

        Arguments:
            action: Single action.
            space: Action space the actions should be present in.

        Returns:
            Clipped batch of actions.
        """

        if isinstance(space, gym.spaces.Box):
            return np.clip(action, space.low, space.high)
        elif isinstance(space, gym.spaces.Tuple):
            if type(action) not in (tuple, list):
                raise ValueError(
                    "Expected tuple space for actions {}: {}".
                    format(action, space))
            out = []
            for a, s in zip(action, space.spaces):
                out.append(Policy.clip_action(a, s))
            return out
        else:
            return action
