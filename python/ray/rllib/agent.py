from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import logging
import numpy as np
import os
import pickle
import tempfile
import time
import uuid

import tensorflow as tf
from ray.tune.logger import UnifiedLogger
from ray.tune.registry import ENV_CREATOR
from ray.tune.result import TrainingResult
from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Agent(Trainable):
    """All RLlib agents extend this base class.

    Agent objects retain internal model state between calls to train(), so
    you should create a new agent instance for each training session.

    Attributes:
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
        registry (obj): Object registry.
    """

    _allow_unknown_configs = False
    _default_logdir = "/tmp/ray"

    def __init__(
            self, config={}, env=None, registry=None, logger_creator=None):
        """Initialize an RLLib agent.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            registry (obj): Object registry for user-defined envs, models, etc.
                If unspecified, it will be assumed empty.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """
        self._initialize_ok = False
        self._experiment_id = uuid.uuid4().hex
        env = env or config.get("env")
        if env:
            config["env"] = env
        if registry and registry.contains(ENV_CREATOR, env):
            self.env_creator = registry.get(ENV_CREATOR, env)
        else:
            import gym
            self.env_creator = lambda: gym.make(env)
        self.config = self._default_config.copy()
        self.registry = registry
        if not self._allow_unknown_configs:
            for k in config.keys():
                if k not in self.config and k != "env":
                    raise Exception(
                        "Unknown agent config `{}`, "
                        "all agent configs: {}".format(k, self.config.keys()))
        self.config.update(config)

        if logger_creator:
            self._result_logger = logger_creator(self.config)
            self.logdir = self._result_logger.logdir
        else:
            logdir_suffix = "{}_{}_{}".format(
                env, self._agent_name,
                datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
            if not os.path.exists(self._default_logdir):
                os.makedirs(self._default_logdir)
            self.logdir = tempfile.mkdtemp(
                prefix=logdir_suffix, dir=self._default_logdir)
            self._result_logger = UnifiedLogger(self.config, self.logdir, None)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = 0

        with tf.Graph().as_default():
            self._init()

        self._initialize_ok = True

    def _init(self, config, env_creator):
        """Subclasses should override this for custom initialization."""

        raise NotImplementedError

    def train(self):
        """Runs one logical iteration of training.

        Returns:
            A TrainingResult that describes training progress.
        """

        if not self._initialize_ok:
            raise ValueError(
                "Agent initialization failed, see previous errors")

        start = time.time()
        result = self._train()
        self._iteration += 1
        if result.time_this_iter_s is not None:
            time_this_iter = result.time_this_iter_s
        else:
            time_this_iter = time.time() - start

        assert result.timesteps_this_iter is not None

        self._time_total += time_this_iter
        self._timesteps_total += result.timesteps_this_iter

        result = result._replace(
            experiment_id=self._experiment_id,
            training_iteration=self._iteration,
            timesteps_total=self._timesteps_total,
            time_this_iter_s=time_this_iter,
            time_total_s=self._time_total,
            pid=os.getpid(),
            hostname=os.uname()[1])

        self._result_logger.on_result(result)

        return result

    def save(self):
        """Saves the current model state to a checkpoint.

        Returns:
            Checkpoint path that may be passed to restore().
        """

        checkpoint_path = self._save()
        pickle.dump(
            [self._experiment_id, self._iteration, self._timesteps_total,
             self._time_total],
            open(checkpoint_path + ".rllib_metadata", "wb"))
        return checkpoint_path

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().
        """

        self._restore(checkpoint_path)
        metadata = pickle.load(open(checkpoint_path + ".rllib_metadata", "rb"))
        self._experiment_id = metadata[0]
        self._iteration = metadata[1]
        self._timesteps_total = metadata[2]
        self._time_total = metadata[3]

    def stop(self):
        """Releases all resources used by this agent."""

        if self._initialize_ok:
            self._result_logger.close()
            self._stop()

    def _stop(self):
        """Subclasses should override this for custom stopping."""

        pass

    def compute_action(self, observation):
        """Computes an action using the current trained policy."""

        raise NotImplementedError

    @property
    def iteration(self):
        """Current training iter, auto-incremented with each train() call."""

        return self._iteration

    @property
    def _agent_name(self):
        """Subclasses should override this to declare their name."""

        raise NotImplementedError

    @property
    def _default_config(self):
        """Subclasses should override this to declare their default config."""

        raise NotImplementedError

    def _train(self):
        """Subclasses should override this to implement train()."""

        raise NotImplementedError

    def _save(self):
        """Subclasses should override this to implement save()."""

        raise NotImplementedError

    def _restore(self):
        """Subclasses should override this to implement restore()."""

        raise NotImplementedError


class _MockAgent(Agent):
    """Mock agent for use in tests"""

    _agent_name = "MockAgent"
    _default_config = {}

    def _init(self):
        self.info = None

    def _train(self):
        return TrainingResult(
            episode_reward_mean=10, episode_len_mean=10,
            timesteps_this_iter=10, info={})

    def _save(self):
        path = os.path.join(self.logdir, "mock_agent.pkl")
        with open(path, 'wb') as f:
            pickle.dump(self.info, f)
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path, 'rb') as f:
            info = pickle.load(f)
        self.info = info

    def set_info(self, info):
        self.info = info
        return info

    def get_info(self):
        return self.info


class _SigmoidFakeData(_MockAgent):
    """Agent that returns sigmoid learning curves.

    This can be helpful for evaluating early stopping algorithms."""

    _agent_name = "SigmoidFakeData"
    _default_config = {
        "width": 100,
        "height": 100,
        "offset": 0,
        "iter_time": 10,
        "iter_timesteps": 1,
    }

    def _train(self):
        i = max(0, self.iteration - self.config["offset"])
        v = np.tanh(float(i) / self.config["width"])
        v *= self.config["height"]
        return TrainingResult(
            episode_reward_mean=v, episode_len_mean=v,
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"], info={})


def get_agent_class(alg):
    """Returns the class of an known agent given its name."""

    if alg == "PPO":
        from ray.rllib import ppo
        return ppo.PPOAgent
    elif alg == "ES":
        from ray.rllib import es
        return es.ESAgent
    elif alg == "DQN":
        from ray.rllib import dqn
        return dqn.DQNAgent
    elif alg == "A3C":
        from ray.rllib import a3c
        return a3c.A3CAgent
    elif alg == "script":
        from ray.tune import script_runner
        return script_runner.ScriptRunner
    elif alg == "__fake":
        return _MockAgent
    elif alg == "__sigmoid_fake_data":
        return _SigmoidFakeData
    else:
        raise Exception(
            ("Unknown algorithm {}, check --alg argument. Valid choices " +
             "are PPO, ES, DQN, and A3C.").format(alg))
