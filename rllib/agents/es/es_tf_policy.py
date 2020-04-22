# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

import gym
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.evaluation.sampler import _unbatch_tuple_actions
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


def rollout(policy, env, timestep_limit=None, add_noise=False, offset=0.0):
    """Do a rollout.

    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.

    Args:
        policy (Policy): Rllib Policy from which to draw actions.
        env (gym.Env): Environment from which to draw rewards, done, and
            next state.
        timestep_limit (Optional[int]): Steps after which to end the rollout.
            If None, use `env.spec.max_episode_steps` or 999999.
        add_noise (bool): Indicates whether exploratory action noise should be
            added.
        offset (float): Value to subtract from the reward (e.g. survival bonus
            from humanoid).
    """
    max_timestep_limit = 999999
    env_timestep_limit = env.spec.max_episode_steps if (
            hasattr(env, "spec") and hasattr(env.spec, "max_episode_steps")) \
        else max_timestep_limit
    timestep_limit = (env_timestep_limit if timestep_limit is None else min(
        timestep_limit, env_timestep_limit))
    rewards = []
    t = 0
    observation = env.reset()
    for _ in range(timestep_limit or max_timestep_limit):
        ac = policy.compute_actions(
            observation, add_noise=add_noise, update=True)[0]
        observation, r, done, _ = env.step(ac)
        if offset != 0.0:
            r -= np.abs(offset)
        rewards.append(r)
        t += 1
        if done:
            break
    rewards = np.array(rewards, dtype=np.float32)
    return rewards, t


def make_session(single_threaded):
    if not single_threaded:
        return tf.Session()
    return tf.Session(
        config=tf.ConfigProto(
            inter_op_parallelism_threads=1, intra_op_parallelism_threads=1))


class ESTFPolicy:
    def __init__(self, obs_space, action_space, config):
        self.action_space = action_space
        self.action_noise_std = config["action_noise_std"]
        self.preprocessor = ModelCatalog.get_preprocessor_for_space(obs_space)
        self.observation_filter = get_filter(config["observation_filter"],
                                             self.preprocessor.shape)
        self.single_threaded = config.get("single_threaded", False)
        self.sess = make_session(single_threaded=self.single_threaded)
        self.inputs = tf.placeholder(tf.float32,
                                     [None] + list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, config["model"], dist_type="deterministic")
        model = ModelCatalog.get_model_v2(#{
            #SampleBatch.CUR_OBS: self.inputs
        #},
            obs_space=obs_space,
            action_space=action_space,
            num_outputs=dist_dim,
            model_config=config["model"])
        dist = dist_class(model.outputs, model)
        self.sampler = dist.sample()

        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            model.outputs, self.sess)

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items())
        self.sess.run(tf.global_variables_initializer())

    def compute_actions(self, observation, add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)
        action = self.sess.run(
            self.sampler, feed_dict={self.inputs: observation})
        action = _unbatch_tuple_actions(action)
        if add_noise and isinstance(self.action_space, gym.spaces.Box):
            action += np.random.randn(*action.shape) * self.action_noise_std
        return action

    def set_flat_weights(self, x):
        self.variables.set_flat(x)

    def get_flat_weights(self):
        return self.variables.get_flat()
