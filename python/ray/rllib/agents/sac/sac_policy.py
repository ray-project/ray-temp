from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Box
import numpy as np
import logging

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.sac.sac_model import SACModel
from ray.rllib.agents.ddpg.noop_model import NoopModel
from ray.rllib.agents.dqn.dqn_policy import _postprocess_dqn, PRIO_WEIGHTS
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.tf_ops import huber_loss, minimize_and_clip

tf = try_import_tf()
logger = logging.getLogger(__name__)


def build_sac_model(policy, obs_space, action_space, config):
    if config["model"]["custom_model"]:
        logger.warning(
            "Setting use_state_preprocessor=True since a custom model "
            "was specified.")
        config["use_state_preprocessor"] = True
    if not isinstance(action_space, Box):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for SAC.".format(action_space))
    if len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space has multiple dimensions "
            "{}. ".format(action_space.shape) +
            "Consider reshaping this into a single dimension, "
            "using a Tuple action space, or the multi-agent API.")

    if config["use_state_preprocessor"]:
        default_model = None  # catalog decides
        num_outputs = 256  # arbitrary
        config["model"]["no_final_linear"] = True
    else:
        default_model = NoopModel
        num_outputs = int(np.product(obs_space.shape))

    policy.model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        model_interface=SACModel,
        default_model=default_model,
        name="sac_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        twin_q=config["twin_q"])

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        model_interface=SACModel,
        default_model=default_model,
        name="target_sac_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        twin_q=config["twin_q"])

    return policy.model


def postprocess_trajectory(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    return _postprocess_dqn(policy, sample_batch)


def exploration_setting_inputs(policy):
    return {
        policy.stochastic: True,
        policy.pure_exploration_phase: policy.cur_pure_exploration_phase,
    }


def build_action_output(policy, model, input_dict, obs_space, action_space,
                        config):
    model_out, _ = model({
        "obs": input_dict[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)
    action_out = model.get_policy_output(model_out)

    # Use sigmoid to scale to [0,1], but also double magnitude of input to
    # emulate behaviour of tanh activation used in SAC and TD3 papers.
    sigmoid_out = tf.nn.sigmoid(2 * action_out)
    # Rescale to actual env policy scale
    # (shape of sigmoid_out is [batch_size, dim_actions], so we reshape to
    # get same dims)
    action_range = (action_space.high - action_space.low)[None]
    low_action = action_space.low[None]
    deterministic_actions = action_range * sigmoid_out + low_action

    noise_type = config["exploration_noise_type"]
    action_low = action_space.low
    action_high = action_space.high
    action_range = action_space.high - action_low
    enable_stochastic = tf.logical_and(policy.stochastic,
                                       not config["parameter_noise"])
    actions = tf.cond(enable_stochastic, compute_stochastic_actions,
                      lambda: deterministic_actions)
    policy.output_actions = actions
    return actions, None


def actor_critic_loss(policy, batch_tensors):
    model_out_t, _ = policy.model({
        "obs": batch_tensors[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    model_out_tp1, _ = policy.model({
        "obs": batch_tensors[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    target_model_out_tp1, _ = policy.target_model({
        "obs": batch_tensors[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    policy_t = policy.model.get_policy_output(model_out_t)
    policy_tp1 = policy.model.get_policy_output(model_out_tp1)

    policy_tp1_smoothed = policy_tp1

    # q network evaluation
    q_t = policy.model.get_q_values(model_out_t,
                                    batch_tensors[SampleBatch.ACTIONS])
    if policy.config["twin_q"]:
        twin_q_t = policy.model.get_twin_q_values(
            model_out_t, batch_tensors[SampleBatch.ACTIONS])

    # Q-values for current policy (no noise) in given current state
    q_t_det_policy = policy.model.get_q_values(model_out_t, policy_t)

    # target q network evaluation
    q_tp1 = policy.target_model.get_q_values(target_model_out_tp1,
                                             policy_tp1_smoothed)
    if policy.config["twin_q"]:
        twin_q_tp1 = policy.target_model.get_twin_q_values(
            target_model_out_tp1, policy_tp1_smoothed)

    q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
    if policy.config["twin_q"]:
        twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

    q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    q_tp1_best_masked = (1.0 - tf.cast(batch_tensors[SampleBatch.DONES],
                                       tf.float32)) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = tf.stop_gradient(
        batch_tensors[SampleBatch.REWARDS] +
        policy.config["gamma"]**policy.config["n_step"] * q_tp1_best_masked)

    # compute the error (potentially clipped)
    if policy.config["twin_q"]:
        td_error = q_t_selected - q_t_selected_target
        twin_td_error = twin_q_t_selected - q_t_selected_target
        td_error = td_error + twin_td_error
        errors = 0.5 * (tf.square(td_error) + tf.square(twin_td_error))
    else:
        td_error = q_t_selected - q_t_selected_target
        errors = 0.5 * tf.square(td_error)

    critic_loss = policy.model.custom_loss(
        tf.reduce_mean(batch_tensors[PRIO_WEIGHTS] * errors), batch_tensors)
    actor_loss = -tf.reduce_mean(q_t_det_policy)

    # save for stats function
    policy.q_t = q_t
    policy.td_error = td_error
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss

    # in a custom apply op we handle the losses separately, but return them
    # combined in one loss for now
    return actor_loss + critic_loss


def gradients(policy, optimizer, loss):
    if policy.config["grad_norm_clipping"] is not None:
        actor_grads_and_vars = minimize_and_clip(
            policy._actor_optimizer,
            policy.actor_loss,
            var_list=policy.model.policy_variables(),
            clip_val=policy.config["grad_norm_clipping"])
        critic_grads_and_vars = minimize_and_clip(
            policy._critic_optimizer,
            policy.critic_loss,
            var_list=policy.model.q_variables(),
            clip_val=policy.config["grad_norm_clipping"])
    else:
        actor_grads_and_vars = policy._actor_optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables())
        critic_grads_and_vars = policy._critic_optimizer.compute_gradients(
            policy.critic_loss, var_list=policy.model.q_variables())
    # save these for later use in build_apply_op
    policy._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                    if g is not None]
    policy._critic_grads_and_vars = [(g, v) for (g, v) in critic_grads_and_vars
                                     if g is not None]
    grads_and_vars = (
        policy._actor_grads_and_vars + policy._critic_grads_and_vars)
    return grads_and_vars


def apply_gradients(policy, optimizer, grads_and_vars):
    # for policy gradient, update policy net one time v.s.
    # update critic net `policy_delay` time(s)
    should_apply_actor_opt = tf.equal(
        tf.mod(policy.global_step, policy.config["policy_delay"]), 0)

    def make_apply_op():
        return policy._actor_optimizer.apply_gradients(
            policy._actor_grads_and_vars)

    actor_op = tf.cond(
        should_apply_actor_opt,
        true_fn=make_apply_op,
        false_fn=lambda: tf.no_op())
    critic_op = policy._critic_optimizer.apply_gradients(
        policy._critic_grads_and_vars)

    # increment global step & apply ops
    with tf.control_dependencies([tf.assign_add(policy.global_step, 1)]):
        return tf.group(actor_op, critic_op)


def stats(policy, batch_tensors):
    return {
        "td_error": tf.reduce_mean(policy.td_error),
        "actor_loss": tf.reduce_mean(policy.actor_loss),
        "critic_loss": tf.reduce_mean(policy.critic_loss),
        "mean_q": tf.reduce_mean(policy.q_t),
        "max_q": tf.reduce_max(policy.q_t),
        "min_q": tf.reduce_min(policy.q_t),
    }


class ExplorationStateMixin(object):
    def __init__(self, obs_space, action_space, config):
        self.cur_pure_exploration_phase = False
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.pure_exploration_phase = tf.placeholder(
            tf.bool, (), name="pure_exploration_phase")

    def set_epsilon(self, epsilon):
        pass

    def set_pure_exploration_phase(self, pure_exploration_phase):
        self.cur_pure_exploration_phase = pure_exploration_phase

    @override(Policy)
    def get_state(self):
        return []

    @override(Policy)
    def set_state(self, state):
        pass


class TargetNetworkMixin(object):
    def __init__(self, config):
        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        self.tau_value = config.get("tau")
        self.tau = tf.placeholder(tf.float32, (), name="tau")
        update_target_expr = []
        model_vars = self.model.trainable_variables()
        target_model_vars = self.target_model.trainable_variables()
        assert len(model_vars) == len(target_model_vars), \
            (model_vars, target_model_vars)
        for var, var_target in zip(model_vars, target_model_vars):
            update_target_expr.append(
                var_target.assign(self.tau * var +
                                  (1.0 - self.tau) * var_target))
            logger.debug("Update target op {}".format(var_target))
        self.update_target_expr = tf.group(*update_target_expr)

        # Hard initial update
        self.update_target(tau=1.0)

    # support both hard and soft sync
    def update_target(self, tau=None):
        tau = tau or self.tau_value
        return self.get_session().run(
            self.update_target_expr, feed_dict={self.tau: tau})


class ActorCriticOptimizerMixin(object):
    def __init__(self, config):
        # create global step for counting the number of update operations
        self.global_step = tf.train.get_or_create_global_step()

        # use separate optimizers for actor & critic
        self._actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["actor_lr"])
        self._critic_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["critic_lr"])


class ComputeTDErrorMixin(object):
    def compute_td_error(self, obs_t, act_t, rew_t, obs_tp1, done_mask,
                         importance_weights):
        if not self.loss_initialized():
            return np.zeros_like(rew_t)

        td_err = self.get_session().run(
            self.td_error,
            feed_dict={
                self.get_placeholder(SampleBatch.CUR_OBS): [
                    np.array(ob) for ob in obs_t
                ],
                self.get_placeholder(SampleBatch.ACTIONS): act_t,
                self.get_placeholder(SampleBatch.REWARDS): rew_t,
                self.get_placeholder(SampleBatch.NEXT_OBS): [
                    np.array(ob) for ob in obs_tp1
                ],
                self.get_placeholder(SampleBatch.DONES): done_mask,
                self.get_placeholder(PRIO_WEIGHTS): importance_weights
            })
        return td_err


def setup_early_mixins(policy, obs_space, action_space, config):
    ExplorationStateMixin.__init__(policy, obs_space, action_space, config)
    ActorCriticOptimizerMixin.__init__(policy, config)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, config)


SACTFPolicy = build_tf_policy(
    name="SACTFPolicy",
    get_default_config=lambda: ray.rllib.agents.sac.sac.DEFAULT_CONFIG,
    make_model=build_sac_model,
    postprocess_fn=postprocess_trajectory,
    extra_action_feed_fn=exploration_setting_inputs,
    action_sampler_fn=build_action_output,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    gradients_fn=gradients,
    apply_gradients_fn=apply_gradients,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    mixins=[
        TargetNetworkMixin, ExplorationStateMixin, ActorCriticOptimizerMixin,
        ComputeTDErrorMixin
    ],
    before_init=setup_early_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False)
