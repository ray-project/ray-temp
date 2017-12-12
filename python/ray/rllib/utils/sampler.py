from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six.moves.queue as queue
import threading
from collections import namedtuple


def lock_wrap(func, lock):
    def wrapper(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)
    return wrapper


class PartialRollout(object):
    """A piece of a complete rollout.

    We run our agent, and process its experience once it has processed enough
    steps.

    Attributes:
        data (dict): Stores rollout data. All numpy arrays other than
            `observations` and `features` will be squeezed.
        last_r (float): Value of next state. Used for bootstrapping.
    """

    fields = ["observations", "actions", "rewards", "terminal", "features"]

    def __init__(self, extra_fields=None):
        """Initializers internals. Maintains a `last_r` field
        in support of partial rollouts, used in bootstrapping advantage
        estimation.

        Args:
            extra_fields: Optional field for object to keep track.
        """
        if extra_fields:
            self.fields.extend(extra_fields)
        self.data = {k: [] for k in self.fields}
        self.last_r = 0.0

    def add(self, **kwargs):
        for k, v in kwargs.items():
            if (k not in ["observations", "features"]
                    and hasattr(v, "squeeze")):
                v = v.squeeze()
            self.data[k] += [v]

    def extend(self, other_rollout):
        """Extends internal data structure. Assumes other_rollout contains
        data that occured afterwards."""

        assert not self.is_terminal()
        assert all(k in other_rollout.fields for k in self.fields)
        for k, v in other_rollout.data.items():
            self.data[k].extend(v)
        self.last_r = other_rollout.last_r

    def is_terminal(self):
        """Check if terminal.

        Returns:
            terminal (bool): if rollout has terminated."""
        return self.data["terminal"][-1]


CompletedRollout = namedtuple(
    "CompletedRollout", ["episode_length", "episode_reward"])


class SyncSampler(object):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches.

    This class provides data on invocation, rather than on a separate
    thread."""
    async = False

    def __init__(
        self, env, policy, obs_filter, num_local_steps, horizon=None):
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.env = env
        self.policy = policy
        self._obs_filter = obs_filter
        self.rollout_provider = _env_runner(
            self.env, self.policy, self.num_local_steps, self.horizon,
            self._obs_filter)
        self.metrics_queue = queue.Queue()

    def get_obs_filter(self, flush=False):
        """Gets a snapshot of the current observation filter. The snapshot
        also by default does not clear the accumulated delta.

        Args:
            flush (bool): If True, accumulated state in buffer is cleared.

        Returns:
            snapshot (Filter): Copy of observation filter.
        """
        snapshot = self._obs_filter.copy()
        if flush and hasattr(self._obs_filter, "clear_buffer"):
            self._obs_filter.clear_buffer()
        return snapshot

    def update_obs_filter(self, other_filter):
        """Updates observation filter with copy from driver.

        Args:
            other_filter: Another filter (of same type).
        """
        self._obs_filter.sync(other_filter)

    def get_data(self):
        while True:
            item = next(self.rollout_provider)
            if isinstance(item, CompletedRollout):
                self.metrics_queue.put(item)
            else:
                return item

    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed


class AsyncSampler(threading.Thread):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches."""
    async = True

    def __init__(
        self, env, policy, obs_filter, num_local_steps, horizon=None):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.env = env
        self.policy = policy
        self._obs_filter = obs_filter
        self._obs_f_lock = threading.Lock()
        self.start()

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def update_obs_filter(self, other_filter):
        """Method to update observation filter with copy from driver.
        Applies delta since last `clear_buffer` to given new filter,
        and syncs current filter to new filter. `self._obs_filter` is
        kept in place due to the `lock_wrap`.

        Args:
            other_filter: Another filter (of same type)."""
        with self._obs_f_lock:
            new_filter = other_filter.copy()
            # Applies delta to filter, including buffer
            new_filter.update(self._obs_filter, copy_buffer=True)
            # copies everything back into original filter - needed
            # due to `lock_wrap`
            self._obs_filter.sync(new_filter)

    def _run(self):
        """Sets observation filter into an atomic region and starts
        other thread for running."""
        safe_obs_filter = lock_wrap(self._obs_filter, self._obs_f_lock)
        rollout_provider = _env_runner(
            self.env, self.policy, self.num_local_steps,
            self.horizon, safe_obs_filter)
        while True:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(rollout_provider)
            if isinstance(item, CompletedRollout):
                self.metrics_queue.put(item)
            else:
                self.queue.put(item, timeout=600.0)

    def get_obs_filter(self, flush=False):
        """Gets a snapshot of the current observation filter. The snapshot
        also clears the accumulated delta. Note that in between getting
        the rollout from self.queue and acquiring the lock here,
        the other thread can run, resulting in slight discrepamcies
        between data retrieved and filter statistics.

        Returns:
            snapshot (Filter): Copy of observation filter.
        """

        with self._obs_f_lock:
            snapshot = self._obs_filter.copy()
            if hasattr(self._obs_filter, "clear_buffer"):
                self._obs_filter.clear_buffer()
        return snapshot

    def get_data(self):
        """Gets currently accumulated data.

        Returns:
            rollout (PartialRollout): trajectory data (unprocessed)
        """

        rollout = self._pull_batch_from_queue()
        return rollout

    def _pull_batch_from_queue(self):
        """Take a rollout from the queue of the thread runner."""
        rollout = self.queue.get(timeout=600.0)
        if isinstance(rollout, BaseException):
            raise rollout
        while not rollout.is_terminal():
            try:
                part = self.queue.get_nowait()
                if isinstance(part, BaseException):
                    raise rollout
                rollout.extend(part)
            except queue.Empty:
                break
        return rollout

    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed


def _env_runner(env, policy, num_local_steps, horizon, obs_filter):
    """This implements the logic of the thread runner.

    It continually runs the policy, and as long as the rollout exceeds a
    certain length, the thread runner appends the policy to the queue. Yields
    when `timestep_limit` is surpassed, environment terminates, or
    `num_local_steps` is reached.

    Args:
        env: Environment generated by env_creator
        policy: Policy used to interact with environment. Also sets fields
            to be included in `PartialRollout`
        num_local_steps: Number of steps before `PartialRollout` is yielded.
        obs_filter: Filter used to process observations.

    Yields:
        rollout (PartialRollout): Object containing state, action, reward,
            terminal condition, and other fields as dictated by `policy`.
    """
    last_observation = obs_filter(env.reset())
    horizon = horizon if horizon else env.spec.tags.get(
        "wrapper_config.TimeLimit.max_episode_steps")
    assert horizon > 0
    if hasattr(policy, "get_initial_features"):
        last_features = policy.get_initial_features()
    else:
        last_features = []
    features = last_features
    length = 0
    rewards = 0
    rollout_number = 0

    while True:
        terminal_end = False
        rollout = PartialRollout(extra_fields=policy.other_output)

        for _ in range(num_local_steps):
            action, pi_info = policy.compute(last_observation, *last_features)
            if policy.is_recurrent:
                features = pi_info["features"]
                del pi_info["features"]
            observation, reward, terminal, info = env.step(action)
            observation = obs_filter(observation)

            length += 1
            rewards += reward
            if length >= horizon:
                terminal = True

            # Collect the experience.
            rollout.add(observations=last_observation,
                        actions=action,
                        rewards=reward,
                        terminal=terminal,
                        features=last_features,
                        **pi_info)

            last_observation = observation
            last_features = features

            if terminal:
                terminal_end = True
                yield CompletedRollout(length, rewards)

                if (length >= horizon or
                        not env.metadata.get("semantics.autoreset")):
                    last_observation = obs_filter(env.reset())
                    if hasattr(policy, "get_initial_features"):
                        last_features = policy.get_initial_features()
                    else:
                        last_features = []
                    rollout_number += 1
                    length = 0
                    rewards = 0
                    break

        if not terminal_end:
            rollout.last_r = policy.value(last_observation, *last_features)

        # Once we have enough experience, yield it, and have the ThreadRunner
        # place it on a queue.
        yield rollout
