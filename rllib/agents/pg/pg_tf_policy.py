"""
TensorFlow policy class used for PG.
"""

from typing import List, Type, Union

import ray
from ray.rllib.agents.pg.utils import post_process_advantages
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.types import TensorType

tf1, tf, tfv = try_import_tf()


def pg_tf_loss(
        policy: Policy,
        model: ModelV2,
        dist_class: Type[ActionDistribution],
        train_batch: SampleBatch) -> Union[TensorType, List[TensorType]]:
    """The basic policy gradients loss function.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]: The action distr. class.
        train_batch (SampleBatch): The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    # Pass the training data through our model.
    logits, _ = model.from_batch(train_batch)

    # Create an action distribution object.
    action_dist = dist_class(logits, model)

    # Calculate the vanilla PG loss.
    return -tf.reduce_mean(
        action_dist.logp(train_batch[SampleBatch.ACTIONS]) * tf.cast(
            train_batch[Postprocessing.ADVANTAGES], dtype=tf.float32))


# Build a child class of `TFPolicy`, given the extra options:
# - trajectory post-processing function (to calculate advantages)
# - PG loss function
PGTFPolicy = build_tf_policy(
    name="PGTFPolicy",
    get_default_config=lambda: ray.rllib.agents.pg.pg.DEFAULT_CONFIG,
    postprocess_fn=post_process_advantages,
    loss_fn=pg_tf_loss)
