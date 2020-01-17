"""
Adapted from VTraceTorchPolicy (TODO)
to use the PPO surrogate loss.
Keep in sync with changes to VTraceTorchPolicy (TODO).
"""

import numpy as np
import logging
import gym

from ray.rllib.agents.impala import vtrace
from ray.rllib.agents.impala.vtrace_policy import _make_time_major, \
        BEHAVIOR_LOGITS, clip_gradients, validate_config, choose_optimizer
from ray.rllib.agents.ppo.appo_tf_policy import build_appo_model, \
    setup_mixins, setup_late_mixins, postprocess_trajectory
from ray.rllib.agents.ppo.ppo_tf_policy import KLCoeffMixin, ValueNetworkMixin
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy, LearningRateSchedule
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.tf_ops import make_tf_callable

torch, _ = try_import_torch()

POLICY_SCOPE = "func"
TARGET_POLICY_SCOPE = "target_func"

logger = logging.getLogger(__name__)


class PPOSurrogateLoss:
    """Loss used when V-trace is disabled.

    Arguments:
        prev_actions_logp: A float32 tensor of shape [T, B].
        actions_logp: A float32 tensor of shape [T, B].
        action_kl: A float32 tensor of shape [T, B].
        actions_entropy: A float32 tensor of shape [T, B].
        values: A float32 tensor of shape [T, B].
        valid_mask: A bool tensor of valid RNN input elements (#2992).
        advantages: A float32 tensor of shape [T, B].
        value_targets: A float32 tensor of shape [T, B].
        vf_loss_coeff (float): Coefficient of the value function loss.
        entropy_coeff (float): Coefficient of the entropy regularizer.
        clip_param (float): Clip parameter.
        cur_kl_coeff (float): Coefficient for KL loss.
        use_kl_loss (bool): If true, use KL loss.
    """

    def __init__(self,
                 prev_actions_logp,
                 actions_logp,
                 action_kl,
                 actions_entropy,
                 values,
                 valid_mask,
                 advantages,
                 value_targets,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01,
                 clip_param=0.3,
                 cur_kl_coeff=None,
                 use_kl_loss=False):

        def reduce_mean_valid(t):
            return torch.mean(t * valid_mask)

        logp_ratio = torch.exp(actions_logp - prev_actions_logp)

        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages * torch.clamp(
                logp_ratio, 1 - clip_param, 1 + clip_param
            )
        )

        self.mean_kl = reduce_mean_valid(action_kl)
        self.pi_loss = -reduce_mean_valid(surrogate_loss)

        # The baseline loss
        delta = values - value_targets
        self.value_targets = value_targets
        self.vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

        # The entropy loss
        self.entropy = reduce_mean_valid(actions_entropy)

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss
        if use_kl_loss:
            self.total_loss += cur_kl_coeff * self.mean_kl


class VTraceSurrogateLoss:
    def __init__(self,
                 actions,
                 prev_actions_logp,
                 actions_logp,
                 old_policy_actions_logp,
                 action_kl,
                 actions_entropy,
                 dones,
                 behavior_logits,
                 old_policy_behavior_logits,
                 target_logits,
                 discount,
                 rewards,
                 values,
                 bootstrap_value,
                 dist_class,
                 model,
                 valid_mask,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01,
                 clip_rho_threshold=1.0,
                 clip_pg_rho_threshold=1.0,
                 clip_param=0.3,
                 cur_kl_coeff=None,
                 use_kl_loss=False):
        """
        APPO Loss, with IS modifications and V-trace for Advantage Estimation.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Arguments:
            actions: An int|float32 tensor of shape [T, B, logit_dim].
            prev_actions_logp: A float32 tensor of shape [T, B].
            actions_logp: A float32 tensor of shape [T, B].
            old_policy_actions_logp: A float32 tensor of shape [T, B].
            action_kl: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behavior_logits: A float32 tensor of shape [T, B, logit_dim].
            old_policy_behavior_logits: A float32 tensor of shape
            [T, B, logit_dim].
            target_logits: A float32 tensor of shape [T, B, logit_dim].
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
            dist_class: action distribution class for logits.
            model: backing ModelV2 instance
            valid_mask: A bool tensor of valid RNN input elements (#2992).
            vf_loss_coeff (float): Coefficient of the value function loss.
            entropy_coeff (float): Coefficient of the entropy regularizer.
            clip_param (float): Clip parameter.
            cur_kl_coeff (float): Coefficient for KL loss.
            use_kl_loss (bool): If true, use KL loss.
        """

        def reduce_mean_valid(t):
            return torch.mean(t * valid_mask)

        # Compute vtrace on the CPU for better perf.
        torch.cuda.device("cpu")
        self.vtrace_returns = vtrace.multi_from_logits(
            behavior_policy_logits=behavior_logits,
            target_policy_logits=old_policy_behavior_logits,
            actions=torch.unstack(actions, axis=2),
            discounts=torch.to_float(~dones) * discount,
            rewards=rewards,
            values=values,
            bootstrap_value=bootstrap_value,
            dist_class=dist_class,
            model=model,
            clip_rho_threshold=torch.astype(
                clip_rho_threshold, torch.float32
            ),
            clip_pg_rho_threshold=torch.astype(
                clip_pg_rho_threshold, torch.float32
            )
        )

        self.is_ratio = torch.clamp(
            torch.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0)
        logp_ratio = self.is_ratio * torch.exp(actions_logp - prev_actions_logp)

        advantages = self.vtrace_returns.pg_advantages
        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages * torch.clamp(
                logp_ratio, 1 - clip_param, 1 + clip_param
            )
        )

        self.mean_kl = reduce_mean_valid(action_kl)
        self.pi_loss = -reduce_mean_valid(surrogate_loss)

        # The baseline loss
        delta = values - self.vtrace_returns.vs
        self.value_targets = self.vtrace_returns.vs
        self.vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

        # The entropy loss
        self.entropy = reduce_mean_valid(actions_entropy)

        # The summed weighted loss
        self.total_loss = self.pi_loss + self.vf_loss * vf_loss_coeff - \
                          self.entropy * entropy_coeff

        # Optional additional KL Loss
        if use_kl_loss:
            self.total_loss += cur_kl_coeff * self.mean_kl


def build_appo_surrogate_loss(policy, model, dist_class, train_batch):
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)

    if isinstance(policy.action_space, gym.spaces.Discrete):
        is_multidiscrete = False
        output_hidden_shape = [policy.action_space.n]
    elif isinstance(policy.action_space,
                    gym.spaces.multi_discrete.MultiDiscrete):
        is_multidiscrete = True
        output_hidden_shape = policy.action_space.nvec.astype(np.int32)
    else:
        is_multidiscrete = False
        output_hidden_shape = 1

    def make_time_major(*args, **kw):
        return _make_time_major(policy, train_batch.get("seq_lens"), *args,
                                **kw)

    actions = train_batch[SampleBatch.ACTIONS]
    dones = train_batch[SampleBatch.DONES]
    rewards = train_batch[SampleBatch.REWARDS]
    behavior_logits = train_batch[BEHAVIOR_LOGITS]

    target_model_out, _ = policy.target_model.from_batch(train_batch)
    old_policy_behavior_logits = target_model_out.detach()

    unpacked_behavior_logits = torch.split(
        behavior_logits, output_hidden_shape, axis=1)
    unpacked_old_policy_behavior_logits = torch.split(
        old_policy_behavior_logits, output_hidden_shape, axis=1)
    unpacked_outputs = torch.split(model_out, output_hidden_shape, axis=1)
    old_policy_action_dist = dist_class(old_policy_behavior_logits, model)
    prev_action_dist = dist_class(behavior_logits, policy.model)
    values = policy.model.value_function()

    policy.model_vars = policy.model.variables()
    policy.target_model_vars = policy.target_model.variables()

    if policy.is_recurrent():
        max_seq_len = torch.max(train_batch["seq_lens"]) - 1
        mask = torch.sequence_mask(train_batch["seq_lens"], max_seq_len)
        mask = mask.view([-1])
    else:
        mask = torch.ones_like(rewards)

    if policy.config["vtrace"]:
        logger.debug("Using V-Trace surrogate loss (vtrace=True)")

        # Prepare actions for loss
        loss_actions = actions if is_multidiscrete else torch.expand_dims(
            actions, axis=1)

        # Prepare KL for Loss
        mean_kl = make_time_major(
            old_policy_action_dist.multi_kl(action_dist), drop_last=True)

        policy.loss = VTraceSurrogateLoss(
            actions=make_time_major(loss_actions, drop_last=True),
            prev_actions_logp=make_time_major(
                prev_action_dist.logp(actions), drop_last=True),
            actions_logp=make_time_major(
                action_dist.logp(actions), drop_last=True),
            old_policy_actions_logp=make_time_major(
                old_policy_action_dist.logp(actions), drop_last=True),
            action_kl=torch.mean(mean_kl, axis=0)
            if is_multidiscrete else mean_kl,
            actions_entropy=make_time_major(
                action_dist.multi_entropy(), drop_last=True),
            dones=make_time_major(dones, drop_last=True),
            behavior_logits=make_time_major(
                unpacked_behavior_logits, drop_last=True),
            old_policy_behavior_logits=make_time_major(
                unpacked_old_policy_behavior_logits, drop_last=True),
            target_logits=make_time_major(unpacked_outputs, drop_last=True),
            discount=policy.config["gamma"],
            rewards=make_time_major(rewards, drop_last=True),
            values=make_time_major(values, drop_last=True),
            bootstrap_value=make_time_major(values)[-1],
            dist_class=Categorical if is_multidiscrete else dist_class,
            model=policy.model,
            valid_mask=make_time_major(mask, drop_last=True),
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_rho_threshold=policy.config["vtrace_clip_rho_threshold"],
            clip_pg_rho_threshold=policy.config[
                "vtrace_clip_pg_rho_threshold"],
            clip_param=policy.config["clip_param"],
            cur_kl_coeff=policy.kl_coeff,
            use_kl_loss=policy.config["use_kl_loss"])
    else:
        logger.debug("Using PPO surrogate loss (vtrace=False)")

        # Prepare KL for Loss
        mean_kl = make_time_major(prev_action_dist.multi_kl(action_dist))

        policy.loss = PPOSurrogateLoss(
            prev_actions_logp=make_time_major(prev_action_dist.logp(actions)),
            actions_logp=make_time_major(action_dist.logp(actions)),
            action_kl=torch.mean(mean_kl, axis=0)
            if is_multidiscrete else mean_kl,
            actions_entropy=make_time_major(action_dist.multi_entropy()),
            values=make_time_major(values),
            valid_mask=make_time_major(mask),
            advantages=make_time_major(train_batch[Postprocessing.ADVANTAGES]),
            value_targets=make_time_major(
                train_batch[Postprocessing.VALUE_TARGETS]),
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_param=policy.config["clip_param"],
            cur_kl_coeff=policy.kl_coeff,
            use_kl_loss=policy.config["use_kl_loss"])

    return policy.loss.total_loss


def stats(policy, train_batch):
    values_batched = _make_time_major(
        policy,
        train_batch.get("seq_lens"),
        policy.model.value_function(),
        drop_last=policy.config["vtrace"])

    stats_dict = {
        "cur_lr": torch.cast(policy.cur_lr, torch.float64),
        "policy_loss": policy.loss.pi_loss,
        "entropy": policy.loss.entropy,
        "var_gnorm": torch.global_norm(policy.model.trainable_variables()),
        "vf_loss": policy.loss.vf_loss,
        "vf_explained_var": explained_variance(
            policy.loss.value_targets.view([-1]),
            values_batched.view([-1])),
    }

    if policy.config["vtrace"]:
        is_stat_mean = torch.mean(policy.loss.is_ratio, dim=[0, 1])
        is_stat_var = torch.var(policy.loss.is_ratio, dim=[0, 1])
        stats_dict.update({"mean_IS": is_stat_mean})
        stats_dict.update({"var_IS": is_stat_var})

    if policy.config["use_kl_loss"]:
        stats_dict.update({"kl": policy.loss.mean_kl})
        stats_dict.update({"KL_Coeff": policy.kl_coeff})

    return stats_dict


def add_values_and_logits(policy):
    out = {BEHAVIOR_LOGITS: policy.model.last_output()}
    if not policy.config["vtrace"]:
        out[SampleBatch.VF_PREDS] = policy.model.value_function()
    return out


class TargetNetworkMixin:
    def __init__(self, obs_space, action_space, config):
        """Target Network is updated by the master learner every
        trainer.update_target_frequency steps. All worker batches
        are importance sampled w.r. to the target network to ensure
        a more stable pi_old in PPO.
        """

        @make_tf_callable(self.get_session())
        def do_update():
            assign_ops = []
            assert len(self.model_vars) == len(self.target_model_vars)
            for var, var_target in zip(self.model_vars,
                                       self.target_model_vars):
                assign_ops.append(var_target.assign(var))
            return tf.group(*assign_ops)

        self.update_target = do_update

    @override(TorchPolicy)
    def variables(self):
        return self.model_vars + self.target_model_vars


AsyncPPOTorchPolicy = build_torch_policy(
    name="AsyncPPOTorchPolicy",
    loss_fn=build_appo_surrogate_loss,
    make_model_and_action_dist=build_appo_model,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    extra_action_out_fn=add_values_and_logits,
    extra_grad_process_fn=clip_gradients,
    optimizer_fn=choose_optimizer,
    before_init=validate_config,
    before_loss_init=setup_mixins,
    after_init=setup_late_mixins,
    mixins=[
        LearningRateSchedule, KLCoeffMixin, TargetNetworkMixin,
        ValueNetworkMixin
    ],
    get_batch_divisibility_req=lambda p: p.config["sample_batch_size"])
