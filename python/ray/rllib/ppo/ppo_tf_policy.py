from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict
import tensorflow as tf

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph
# from ray.rllib.utils.policy_graph import PolicyGraph

class PPOLoss(object):
    def __init__(self, inputs, ac_space, curr_dist, value_fn, entropy_coeff=0,
        clip_param=0.1, vf_loss_coeff=0.0, use_gae=True):
        dist_cls, _ = ModelCatalog.get_action_dist(ac_space)
        # The coefficient of the KL penalty.
        self.kl_coeff = tf.placeholder(
            name="newkl", shape=(), dtype=tf.float32)
        self.prev_dist = dist_cls(inputs["logprobs"])
        # Make loss functions.
        self.ratio = tf.exp(curr_dist.logp(inputs["actions"]) -
                            self.prev_dist.logp(inputs["actions"]))
        self.kl = self.prev_dist.kl(curr_dist)
        self.mean_kl = tf.reduce_mean(self.kl)
        self.entropy = curr_dist.entropy()
        self.mean_entropy = tf.reduce_mean(self.entropy)
        self.surr1 = self.ratio * inputs["advantages"]
        self.surr2 =  inputs["advantages"] * tf.clip_by_value(
            self.ratio, 1 - clip_param,
            1 + clip_param)
        self.surr = tf.minimum(self.surr1, self.surr2)
        self.mean_policy_loss = tf.reduce_mean(-self.surr)

        if use_gae:
            # We use a huber loss here to be more robust against outliers,
            # which seem to occur when the rollouts get longer (the variance
            # scales superlinearly with the length of the rollout)
            self.vf_loss1 = tf.square(value_fn - inputs["value_targets"])
            vf_clipped = inputs["vf_preds"] + tf.clip_by_value(
                value_fn - inputs["vf_preds"],
                -clip_param, clip_param)
            self.vf_loss2 = tf.square(vf_clipped - inputs["value_targets"])
            self.vf_loss = tf.minimum(self.vf_loss1, self.vf_loss2)
            self.mean_vf_loss = tf.reduce_mean(self.vf_loss)
            loss = tf.reduce_mean(
                -self.surr + self.kl_coeff * self.kl +
                vf_loss_coeff * self.vf_loss -
                entropy_coeff * self.entropy)
        else:
            self.mean_vf_loss = tf.constant(0.0)
            loss = tf.reduce_mean(
                -self.surr +
                self.kl_coeff * self.kl -
                entropy_coeff * self.entropy)
        self.loss = loss

    def extra_fetches(self):
        return {"kl": self.mean_kl}


class PPOTFPolicyGraph(TFPolicyGraph):
    """PPO Graph"""

    def __init__(self, ob_space, action_space, config, loss_in=None):
        self.config = config
        if loss_in:
            self._inputs = OrderedDict(loss_in)
            self.loss_in = loss_in
        else:
            self._setup_inputs(ob_space, action_space)
        self._setup_graph(action_space)
        print("Setting up loss")
        loss = PPOLoss(self._inputs, action_space,
                            self.curr_dist, self.value_function, use_gae=config["use_gae"])
        self.loss = loss.loss
        self.is_training = tf.placeholder_with_default(True, ())
        self.sess = tf.get_default_session()

        TFPolicyGraph.__init__(
            self, self.sess, obs_input=self._inputs["obs"],
            action_sampler=self.sampler, loss=self.loss,
            loss_inputs=self.loss_in, is_training=self.is_training)

    def _setup_inputs(self, ob_space, action_space):
        _, logit_dim = ModelCatalog.get_action_dist(action_space)
        # Defines the training inputs:
        self._inputs = OrderedDict()
        self._inputs["obs"] = tf.placeholder(
            tf.float32, shape=(None,) + ob_space.shape)
        # Targets of the value function.
        self._inputs["value_targets"] = tf.placeholder(tf.float32, shape=(None,))
        # Advantage values in the policy gradient estimator.
        self._inputs["advantages"] = tf.placeholder(tf.float32, shape=(None,))
        self._inputs["actions"] = ModelCatalog.get_action_placeholder(action_space)
        # Log probabilities from the policy before the policy update.
        self._inputs["logprobs"] = tf.placeholder(
            tf.float32, shape=(None, logit_dim))
        # Value function predictions before the policy update.
        self._inputs["vf_preds"] = tf.placeholder(tf.float32, shape=(None,))
        self.loss_in = list(self._inputs.items())

    def _setup_graph(self, action_space):
        self.dist_cls, self.logit_dim = ModelCatalog.get_action_dist(action_space)
        self.logits = ModelCatalog.get_model(
            self._inputs["obs"], self.logit_dim, self.config["model"]).outputs
        self.curr_dist = self.dist_cls(self.logits)
        self.sampler = self.curr_dist.sample()
        if self.config["use_gae"]:
            vf_config = self.config["model"].copy()
            # Do not split the last layer of the value function into
            # mean parameters and standard deviation parameters and
            # do not make the standard deviations free variables.
            vf_config["free_log_std"] = False
            with tf.variable_scope("value_function"):
                self.value_function = ModelCatalog.get_model(
                    self._inputs["obs"], 1, vf_config).outputs
            self.value_function = tf.reshape(self.value_function, [-1])
        else:
            self.value_function = tf.constant("NA")

    def extra_compute_action_fetches(self):
        return {"vf_preds": self.value_function, "logprobs": self.logits}

    def extra_compute_grad_fetches(self):
        return self.loss.extra_fetches()

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        last_r = 0.0
        batch = compute_advantages(
            sample_batch, last_r, self.config["gamma"], self.config["lambda"])
        return batch

    def gradients(self, optimizer):
        return optimizer.compute_gradients(
            self._loss, colocate_gradients_with_ops=True)

    def initialize_gradients(self):
        pass
