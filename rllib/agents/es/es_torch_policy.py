# Code in this file is adapted from:
# https://github.com/openai/evolution-strategies-starter.

import gym
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.es.es import DEFAULT_CONFIG
from ray.rllib.evaluation.sampler import _unbatch_tuple_actions
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.filter import get_filter
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


def before_init(policy, observation_space, action_space, config):
    policy.action_noise_std = config["action_noise_std"]
    policy.preprocessor = ModelCatalog.get_preprocessor_for_space(
        observation_space)
    policy.observation_filter = get_filter(
        config["observation_filter"], policy.preprocessor.shape)
    policy.single_threaded = config.get("single_threaded", False)

    def _set_flat_weights(policy, theta):
        print(theta.shape)
        pos = 0
        theta_dict = policy.model.state_dict()
        for k in sorted(theta_dict.keys()):
            print("key={}".format(k))
            shape = policy.param_shapes[k]
            print("shape={}".format(shape))
            num_params = np.prod(shape)
            theta_dict[k].data = torch.from_numpy(np.reshape(theta[pos:pos+num_params], shape))
            pos += num_params
        policy.model.load_state_dict(theta_dict)

    def _get_flat_weights(policy):
        # Get the parameter tensors.
        theta_dict = policy.model.state_dict()
        # Flatten it into a single np.ndarray.
        theta_list = []
        for k in sorted(theta_dict.keys()):
            theta_list.append(torch.reshape(theta_dict[k], (-1, )))
        cat = torch.cat(theta_list, dim=0)
        return cat.numpy()

    type(policy).set_flat_weights = _set_flat_weights
    type(policy).get_flat_weights = _get_flat_weights


def after_init(policy, observation_space, action_space, config):
    #policy.variables = policy.model.variables()
    state_dict = policy.model.state_dict()
    policy.param_shapes = {k: tuple(state_dict[k].size()) for k in sorted(state_dict.keys())}
    policy.num_params = sum(np.prod(s) for s in policy.param_shapes.values())
    print(policy.num_params)


def make_model_and_action_dist(
        policy, observation_space, action_space, config):
    # Policy network.
    dist_class, dist_dim = ModelCatalog.get_action_dist(
        action_space,
        config["model"],  # model_options
        dist_type="deterministic",
        framework="torch")
    model = ModelCatalog.get_model_v2(
        observation_space,
        action_space,
        num_outputs=dist_dim,
        model_config=config["model"],
        framework="torch")
    # Make all model params not require any gradients.
    for p in model.parameters():
        p.requires_grad = False
    return model, dist_class


def action_sampler_fn(policy, model, obs_batch, explore=True,
                      is_training=False, add_noise=False, update=True):
    observation = policy.preprocessor.transform(obs_batch)
    observation = policy.observation_filter(observation[None], update=update)

    dist_inputs, _ = model(observation, [], None)
    dist = policy.dist_class(dist_inputs, model)
    action = dist.sample()
    action = _unbatch_tuple_actions(action)
    if add_noise and isinstance(policy.action_space, gym.spaces.Box):
        action += np.random.randn(*action.shape) * policy.action_noise_std
    return action


ESTorchPolicy = build_torch_policy(
    name="ESTorchPolicy",
    loss_fn=None,
    get_default_config=lambda: ray.rllib.agents.es.es.DEFAULT_CONFIG,
    before_init=before_init,
    after_init=after_init,
    action_sampler_fn=action_sampler_fn,
    make_model_and_action_dist=make_model_and_action_dist
)
