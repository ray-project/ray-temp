from abc import abstractmethod, ABCMeta
import logging
from typing import Dict, List, Union

from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.typing import AgentID, EnvID, EpisodeID, PolicyID, \
    TensorType

logger = logging.getLogger(__name__)


class _SampleCollector(metaclass=ABCMeta):
    """Collects samples for all policies and agents from a multi-agent env.

    Note: This is an experimental class only used when
    `config._use_trajectory_view_api` = True.
    Once `_use_trajectory_view_api` becomes the default in configs:
    This class will deprecate the `SampleBatchBuilder` and
    `MultiAgentBatchBuilder` classes.

    This API is controlled by RolloutWorker objects to store all data
    generated by Environments and Policies/Models during rollout and
    postprocessing. It's purposes are to a) make data collection and
    SampleBatch/input_dict generation from this data faster, b) to unify
    the way we collect samples from environments and model (outputs), thereby
    allowing for possible user customizations, c) to allow for more complex
    inputs fed into different policies (e.g. multi-agent case with inter-agent
    communication channel).
    """

    @abstractmethod
    def add_init_obs(self, episode: MultiAgentEpisode, agent_id: AgentID,
                     policy_id: PolicyID, t: int,
                     init_obs: TensorType) -> None:
        """Adds an initial obs (after reset) to this collector.

        Since the very first observation in an environment is collected w/o
        additional data (w/o actions, w/o reward) after env.reset() is called,
        this method initializes a new trajectory for a given agent.
        `add_init_obs()` has to be called first for each agent/episode-ID
        combination. After this, only `add_action_reward_next_obs()` must be
        called for that same agent/episode-pair.

        Args:
            episode (MultiAgentEpisode): The MultiAgentEpisode, for which we
                are adding an Agent's initial observation.
            agent_id (AgentID): Unique id for the agent we are adding
                values for.
            env_id (EnvID): The environment index (in a vectorized setup).
            policy_id (PolicyID): Unique id for policy controlling the agent.
            t (int): The time step (episode length - 1). The initial obs has
                ts=-1(!), then an action/reward/next-obs at t=0, etc..
            init_obs (TensorType): Initial observation (after env.reset()).

        Examples:
            >>> obs = env.reset()
            >>> collector.add_init_obs(12345, 0, "pol0", obs)
            >>> obs, r, done, info = env.step(action)
            >>> collector.add_action_reward_next_obs(12345, 0, "pol0", False, {
            ...     "action": action, "obs": obs, "reward": r, "done": done
            ... })
        """
        raise NotImplementedError

    @abstractmethod
    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID, env_id: EnvID,
                                   policy_id: PolicyID, agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this collector.

        The incoming data (`values`) must include action, reward, done, and
        next_obs information and may include any other information.
        For the initial observation (after Env.reset()) of the given agent/
        episode-ID combination, `add_initial_obs()` must be called instead.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are adding
                values for.
            agent_id (AgentID): Unique id for the agent we are adding
                values for.
            env_id (EnvID): The environment index (in a vectorized setup).
            policy_id (PolicyID): Unique id for policy controlling the agent.
            agent_done (bool): Whether the given agent is done with its
                trajectory (the multi-agent episode may still be ongoing).
            values (Dict[str, TensorType]): Row of values to add for this
                agent. This row must contain the keys SampleBatch.ACTION,
                REWARD, NEW_OBS, and DONE.

        Examples:
            >>> obs = env.reset()
            >>> collector.add_init_obs(12345, 0, "pol0", obs)
            >>> obs, r, done, info = env.step(action)
            >>> collector.add_action_reward_next_obs(12345, 0, "pol0", False, {
            ...     "action": action, "obs": obs, "reward": r, "done": done
            ... })
        """
        raise NotImplementedError

    @abstractmethod
    def episode_step(self, episode_id: EpisodeID) -> None:
        """Increases the episode step counter (across all agents) by one.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are stepping
                through (across all agents in that episode).
        """
        raise NotImplementedError

    @abstractmethod
    def total_env_steps(self) -> int:
        """Returns total number of steps taken in the env (sum of all agents).

        Returns:
            int: The number of steps taken in total in the environment over all
                agents.
        """
        raise NotImplementedError

    @abstractmethod
    def get_inference_input_dict(self, policy_id: PolicyID) -> \
            Dict[str, TensorType]:
        """Returns an input_dict for an (inference) forward pass from our data.

        The input_dict can then be used for action computations inside a
        Policy via `Policy.compute_actions_from_input_dict()`.

        Args:
            policy_id (PolicyID): The Policy ID to get the input dict for.

        Returns:
            Dict[str, TensorType]: The input_dict to be passed into the ModelV2
                for inference/training.

        Examples:
            >>> obs, r, done, info = env.step(action)
            >>> collector.add_action_reward_next_obs(12345, 0, "pol0", {
            ...     "action": action, "obs": obs, "reward": r, "done": done
            ... })
            >>> input_dict = collector.get_inference_input_dict(policy.model)
            >>> action = policy.compute_actions_from_input_dict(input_dict)
            >>> # repeat
        """
        raise NotImplementedError

    @abstractmethod
    def postprocess_episode(self,
                            episode: MultiAgentEpisode,
                            is_done: bool = False,
                            check_dones: bool = False) -> None:
        """Postprocesses all agents' trajectories in a given episode.

        Generates (single-trajectory) SampleBatches for all Policies/Agents and
        calls Policy.postprocess_trajectory on each of these. Postprocessing
        may happens in-place, meaning any changes to the viewed data columns
        are directly reflected inside this collector's buffers.
        Also makes sure that additional (newly created) data columns are
        correctly added to the buffers.

        Args:
            episode (MultiAgentEpisode): The Episode object for which
                to post-process data.
            is_done (bool): Whether the given episode is actually terminated
                (all agents are done).
            check_dones (bool): Whether we need to check that all agents'
                trajectories have dones=True at the end.
        """
        raise NotImplementedError

    @abstractmethod
    def build_multi_agent_batch(self, env_steps: int, env_index: int) -> \
            Union[MultiAgentBatch, SampleBatch]:
        """Builds a MultiAgentBatch of size=env_steps from the collected data.

        Args:
            env_steps (int): The sum of all env-steps (across all agents) taken
                so far.
            env_index (int): The environment index (in a vector env) for which
                to build the batch.

        Returns:
            Union[MultiAgentBatch, SampleBatch]: Returns the accumulated
                sample batches for each policy inside one MultiAgentBatch
                object (or a simple SampleBatch if only one policy).
        """
        raise NotImplementedError

    @abstractmethod
    def try_build_truncated_episode_multi_agent_batch(self) -> \
            List[Union[MultiAgentBatch, SampleBatch]]:
        """Tries to build an MA-batch, if `rollout_fragment_length` is reached.

        Any unprocessed data will be first postprocessed with a policy
        postprocessor.
        This is usually called to collect samples for policy training.
        If not enough data has been collected yet (`rollout_fragment_length`),
        returns None.

        Returns:
            List[Union[MultiAgentBatch, SampleBatch]]: Returns a (possibly
                empty) list of MultiAgentBatches (containing the accumulated
                SampleBatches for each policy or a simple SampleBatch if only
                one policy). The list will be empty if
                `self.rollout_fragment_length` has not been reached yet.
        """
        raise NotImplementedError
