"""Note: Keep in sync with changes to VTraceTFPolicy."""
from typing import Optional, Type
import gym

import ray
from ray.rllib.agents.ppo.ppo_tf_policy import ValueNetworkMixin
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch, \
    Postprocessing
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_policy import LearningRateSchedule
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_ops import explained_variance
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import TrainerConfigDict, TensorType, \
    PolicyID, LocalOptimizer, ModelGradients
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.evaluation import MultiAgentEpisode

tf1, tf, tfv = try_import_tf()


def postprocess_advantages(policy: Policy,
                   sample_batch: SampleBatch,
                   other_agent_batches: Optional[Dict[PolicyID, SampleBatch]]=None,
                   episode: Optional[MultiAgentEpisode]=None) -> SampleBatch:

    # Stub serving backward compatibility.
    deprecation_warning(
        old="rllib.agents.a3c.a3c_tf_policy.postprocess_advantages",
        new="rllib.evaluation.postprocessing.compute_gae_for_sample_batch",
        error=False)

    return compute_gae_for_sample_batch(policy, sample_batch,
                                        other_agent_batches, episode)


class A3CLoss:
    def __init__(self,
                 action_dist; ActionDistribution,
                 actions: TensorType,
                 advantages: TensorType,
                 v_target: TensorType,
                 vf: TensorType,
                 vf_loss_coeff: float =0.5,
                 entropy_coeff: float =0.01):
        log_prob = action_dist.logp(actions)

        # The "policy gradients" loss
        self.pi_loss = -tf.reduce_sum(log_prob * advantages)

        delta = vf - v_target
        self.vf_loss = 0.5 * tf.reduce_sum(tf.math.square(delta))
        self.entropy = tf.reduce_sum(action_dist.entropy())
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)


def actor_critic_loss(policy: Policy, model: ModelV2, dist_class: ActionDistribution, train_batch: SampleBatch) -> TensorType:
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)
    policy.loss = A3CLoss(action_dist, train_batch[SampleBatch.ACTIONS],
                          train_batch[Postprocessing.ADVANTAGES],
                          train_batch[Postprocessing.VALUE_TARGETS],
                          model.value_function(),
                          policy.config["vf_loss_coeff"],
                          policy.config["entropy_coeff"])
    return policy.loss.total_loss


def add_value_function_fetch(policy: Policy) -> Dict[str, TensorType]:
    return {SampleBatch.VF_PREDS: policy.model.value_function()}


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    return {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "policy_loss": policy.loss.pi_loss,
        "policy_entropy": policy.loss.entropy,
        "var_gnorm": tf.linalg.global_norm(
            list(policy.model.trainable_variables())),
        "vf_loss": policy.loss.vf_loss,
    }


def grad_stats(policy: Policy, train_batch: SampleBatch, grads: ModelGradients) -> Dict[str, TensorType]:
    return {
        "grad_gnorm": tf.linalg.global_norm(grads),
        "vf_explained_var": explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS],
            policy.model.value_function()),
    }


def clip_gradients(policy: Policy, optimizer: LocalOptimizer, loss: TensorType) -> ModelGradients:
    grads_and_vars = optimizer.compute_gradients(
        loss, policy.model.trainable_variables())
    grads = [g for (g, v) in grads_and_vars]
    grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
    clipped_grads = list(zip(grads, policy.model.trainable_variables()))
    return clipped_grads


def setup_mixins(policy: Policy, obs_space: gym.spaces.Space, action_space: gym.spaces.Space, config: TrainerConfigDict) -> None:
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


A3CTFPolicy = build_tf_policy(
    name="A3CTFPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    grad_stats_fn=grad_stats,
    gradients_fn=clip_gradients,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_action_out_fn=add_value_function_fetch,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin, LearningRateSchedule])
