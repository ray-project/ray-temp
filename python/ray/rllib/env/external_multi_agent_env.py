from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from six.moves import queue
import threading
import uuid

from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class ExternalMultiAgentEnv(threading.Thread):
    """An environment that interfaces with external agents.

    Unlike simulator envs, control is inverted. The environment queries the
    policy to obtain actions and logs observations and rewards for training.
    This is in contrast to gym.Env, where the algorithm drives the simulation
    through env.step() calls.

    You can use ExternalEnv as the backend for policy serving (by serving HTTP
    requests in the run loop), for ingesting offline logs data (by reading
    offline transitions in the run loop), or other custom use cases not easily
    expressed through gym.Env.

    ExternalEnv supports both on-policy actions (through self.get_action()),
    and off-policy actions (through self.log_action()).

    This env is thread-safe, but individual episodes must be executed serially.

    Attributes:
        action_space (gym.Space): Action space.
        observation_space (gym.Space): Observation space.

    Examples:
        >>> register_env("my_env", lambda config: YourExternalEnv(config))
        >>> agent = DQNAgent(env="my_env")
        >>> while True:
              print(agent.train())
    """

    @PublicAPI
    def __init__(self, action_space, observation_space, max_concurrent=100):
        """Initialize an external env.

        ExternalEnv subclasses must call this during their __init__.

        Arguments:
            action_space (gym.Space): Action space of the env.
            observation_space (gym.Space): Observation space of the env.
            max_concurrent (int): Max number of active episodes to allow at
                once. Exceeding this limit raises an error.
        """

        threading.Thread.__init__(self)
        self.daemon = True
        self.action_space = action_space
        self.observation_space = observation_space
        self._episodes = {}
        self._finished = set()
        self._results_avail_condition = threading.Condition()
        self._max_concurrent_episodes = max_concurrent

        # we require to know all agents' spaces
        if isinstance(self.action_space, dict) or isinstance(self.observation_space, dict):
            assert(self.action_space.keys() == self.observation_space.keys())

    @PublicAPI
    def run(self):
        """Override this to implement the run loop.

        Your loop should continuously:
            1. Call self.start_episode(episode_id)
            2. Call self.get_action(episode_id, obs)
                    -or-
                    self.log_action(episode_id, obs, action)
            3. Call self.log_returns(episode_id, reward)
            4. Call self.end_episode(episode_id, obs)
            5. Wait if nothing to do.

        Multiple episodes may be started at the same time.
        """
        raise NotImplementedError

    @PublicAPI
    def start_episode(self, episode_id=None, training_enabled=True):
        """Record the start of an episode.

        Arguments:
            episode_id (str): Unique string id for the episode or None for
                it to be auto-assigned.
            training_enabled (bool): Whether to use experiences for this
                episode to improve the policy.

        Returns:
            episode_id (str): Unique string id for the episode.
        """

        if episode_id is None:
            episode_id = uuid.uuid4().hex

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id in self._episodes:
            raise ValueError(
                "Episode {} is already started".format(episode_id))

        self._episodes[episode_id] = _ExternalEnvEpisode(
            episode_id, self._results_avail_condition, training_enabled)

        return episode_id

    @PublicAPI
    def get_action(self, episode_id, observation_dict):
        """Record an observation and get the on-policy action.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.

        Returns:
            action (dict): Action from the env action space.
        """

        # FIXME handle different agents here
        episode = self._get(episode_id)
        return episode.wait_for_action(observation_dict)

    @PublicAPI
    def log_action(self, episode_id, observation_dict, action_dict):
        """Record an observation and (off-policy) action taken.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
            action_dict (dict): Action for the observation.
        """

        # FIXME handle different agents here
        episode = self._get(episode_id)
        episode.log_action(observation_dict, action_dict)

    @PublicAPI
    def log_returns(self, episode_id, reward_dict, info_dict=None):
        """Record returns from the environment.

        The reward will be attributed to the previous action taken by the
        episode. Rewards accumulate until the next action. If no reward is
        logged before the next action, a reward of 0.0 is assumed.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            reward_dict (dict): Reward from the environment agents.
            info (dict): Optional info dict.
        """

        episode = self._get(episode_id)

        # accumulate reward by agent
        # for existing agents, we want to add the reward up
        for agent, rew in reward_dict.items():
            if agent in episode.cur_reward_dict:
                episode.cur_reward_dict[agent] += rew
            else:
                episode.cur_reward_dict[agent] = rew 
        if info_dict:
            episode.cur_info_dict = info_dict or {}

    @PublicAPI
    def end_episode(self, episode_id, observation_dict):
        """Record the end of an episode.

        Arguments:
            episode_id (str): Episode id returned from start_episode().
            observation_dict (dict): Current environment observation.
        """

        episode = self._get(episode_id)
        self._finished.add(episode.episode_id)
        episode.done(observation_dict)

    def _get(self, episode_id):
        """Get a started episode or raise an error."""

        if episode_id in self._finished:
            raise ValueError(
                "Episode {} has already completed.".format(episode_id))

        if episode_id not in self._episodes:
            raise ValueError("Episode {} not found.".format(episode_id))

        return self._episodes[episode_id]


class _ExternalEnvEpisode(object):
    """
    Tracked state for each active episode.
    FIXME fix this class:
    - multiagent dict variables
    """

    def reset_cur_done_dict(self, done=False):
        self.cur_done_dict = {"__all__": done}

    def __init__(self, episode_id, results_avail_condition, training_enabled):
        self.episode_id = episode_id
        self.results_avail_condition = results_avail_condition
        self.training_enabled = training_enabled
        self.data_queue = queue.Queue()
        self.action_queue = queue.Queue()
        self.new_observation_dict = None
        self.new_action_dict = None

        self.cur_reward_dict = {}

        self.reset_cur_done_dict()
        self.cur_info_dict = {}

    def get_data(self):
        if self.data_queue.empty():
            return None
        return self.data_queue.get_nowait()

    def log_action(self, observation_dict, action_dict):
        self.new_observation_dict = observation_dict
        self.new_action_dict = action_dict
        self._send()
        self.action_queue.get(True, timeout=60.0)

    def wait_for_action(self, observation_dict):
        self.new_observation_dict = observation_dict
        self._send()
        return self.action_queue.get(True, timeout=60.0)

    def done(self, observation_dict):
        self.new_observation_dict = observation_dict
        self.reset_cur_done_dict(True)
        self._send()

    def _send(self):
        item = {
            "obs": self.new_observation_dict,
            "reward": self.cur_reward_dict,
            "done": self.cur_done_dict,
            "info": self.cur_info_dict,
        }
        if self.new_action_dict is not None:
            item["off_policy_action"] = self.new_action_dict
        if not self.training_enabled:
            item["info"]["training_enabled"] = False
        self.new_observation_dict = None
        self.new_action_dict = None
        self.cur_reward_dict = {}
        with self.results_avail_condition:
            self.data_queue.put_nowait(item)
            self.results_avail_condition.notify()
