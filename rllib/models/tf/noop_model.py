from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class OBSOLETE_TFNoopModel(TFModelV2):
    """Trivial model that just returns the obs flattened.

    This is the model used if use_state_preprocessor=False."""

    @override(TFModelV2)
    def forward(self, input_dict, state, seq_lens):
        return tf.cast(input_dict["obs_flat"], tf.float32), state
