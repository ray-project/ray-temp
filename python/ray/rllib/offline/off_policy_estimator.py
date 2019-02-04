from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import logging

from ray.rllib.evaluation.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

OffPolicyEstimate = namedtuple("OffPolicyEstimate",
                               ["estimator_name", "metrics"])


@DeveloperAPI
class OffPolicyEstimator(object):
    """Interface for an off policy reward estimator (experimental)."""

    @DeveloperAPI
    def __init__(self, ioctx):
        self.ioctx = ioctx
        self.gamma = ioctx.evaluator.policy_config["gamma"]

        # Grab a reference to the current model
        keys = list(ioctx.evaluator.policy_map.keys())
        if len(keys) > 1:
            logger.warning(
                "Offline estimation is not implemented for multi-agent")
            self.policy = None
        else:
            self.policy = ioctx.evaluator.get_policy(keys[0])

        # Buffer of metrics that will be collected by the driver
        self.estimates = []

    @DeveloperAPI
    def process(self, batch):
        """Process a new batch of experiences.

        The batch will only contain data from one episode, but it may only be
        a fragment of an episode.
        """
        raise NotImplementedError

    @DeveloperAPI
    def check_can_estimate_for(self, batch):
        """Returns whether we can support OPE for this batch."""

        if isinstance(batch, MultiAgentBatch):
            raise ValueError(
                "IS-estimation is not implemented for multi-agent batches. "
                "You can set `input_evaluation: []` to resolve this.")

        if "action_prob" not in batch:
            raise ValueError(
                "Off-policy estimation is not possible unless the inputs "
                "include action probabilities (i.e., the policy is stochastic "
                "and emits the 'action_prob' key). You can set "
                "`input_evaluation: []` to resolve this.")

    @DeveloperAPI
    def action_prob(self, batch):
        """Returns the probs for the batch actions for the current policy."""

        num_state_inputs = 0
        for k in batch.keys():
            if k.startswith("state_in_"):
                num_state_inputs += 1
        state_keys = ["state_in_{}".format(i) for i in range(num_state_inputs)]
        _, _, info = self.policy.compute_actions(
            obs_batch=batch["obs"],
            state_batches=[batch[k] for k in state_keys],
            prev_action_batch=batch.data.get("prev_action"),
            prev_reward_batch=batch.data.get("prev_reward"),
            info_batch=batch.data.get("info"))
        if "action_prob" not in info:
            raise ValueError(
                "Off-policy estimation is not possible unless the policy "
                "returns action probabilities when computing actions (i.e., "
                "the 'action_prob' key is output by the policy graph). You "
                "can set `input_evaluation: []` to resolve this.")
        return info["action_prob"]

    @DeveloperAPI
    def get_metrics(self):
        """Return a list of new episode metric estimates since the last call.

        Returns:
            list of OffPolicyEstimate objects.
        """
        out = self.estimates
        self.estimates = []
        return out
