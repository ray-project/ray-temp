import numpy as np
import gym
from typing import Dict, List

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.policy.rnn_sequencing import add_time_dimension, \
    chop_into_sequences
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import ModelConfigDict, TensorType

tf1, tf, tfv = try_import_tf()


@DeveloperAPI
class RecurrentNetwork(TFModelV2):
    """Helper class to simplify implementing RNN models with TFModelV2.

    Instead of implementing forward(), you can implement forward_rnn() which
    takes batches with the time dimension added already.

    Here is an example implementation for a subclass
    ``MyRNNClass(RecurrentNetwork)``::

        def __init__(self, *args, **kwargs):
            super(MyModelClass, self).__init__(*args, **kwargs)
            cell_size = 256

            # Define input layers
            input_layer = tf.keras.layers.Input(
                shape=(None, obs_space.shape[0]))
            state_in_h = tf.keras.layers.Input(shape=(256, ))
            state_in_c = tf.keras.layers.Input(shape=(256, ))
            seq_in = tf.keras.layers.Input(shape=(), dtype=tf.int32)

            # Send to LSTM cell
            lstm_out, state_h, state_c = tf.keras.layers.LSTM(
                cell_size, return_sequences=True, return_state=True,
                name="lstm")(
                    inputs=input_layer,
                    mask=tf.sequence_mask(seq_in),
                    initial_state=[state_in_h, state_in_c])
            output_layer = tf.keras.layers.Dense(...)(lstm_out)

            # Create the RNN model
            self.rnn_model = tf.keras.Model(
                inputs=[input_layer, seq_in, state_in_h, state_in_c],
                outputs=[output_layer, state_h, state_c])
            self.register_variables(self.rnn_model.variables)
            self.rnn_model.summary()
    """

    @override(ModelV2)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        """Adds time dimension to batch before sending inputs to forward_rnn().

        You should implement forward_rnn() in your subclass."""
        assert seq_lens is not None
        padded_inputs = input_dict["obs_flat"]
        max_seq_len = tf.shape(padded_inputs)[0] // tf.shape(seq_lens)[0]
        output, new_state = self.forward_rnn(
            add_time_dimension(
                padded_inputs, max_seq_len=max_seq_len, framework="tf"), state,
            seq_lens)
        return tf.reshape(output, [-1, self.num_outputs]), new_state

    def forward_rnn(self, inputs: TensorType, state: List[TensorType],
                    seq_lens: TensorType) -> (TensorType, List[TensorType]):
        """Call the model with the given input tensors and state.

        Args:
            inputs (dict): observation tensor with shape [B, T, obs_size].
            state (list): list of state tensors, each with shape [B, T, size].
            seq_lens (Tensor): 1d tensor holding input sequence lengths.

        Returns:
            (outputs, new_state): The model output tensor of shape
                [B, T, num_outputs] and the list of new state tensors each with
                shape [B, size].

        Sample implementation for the ``MyRNNClass`` example::

            def forward_rnn(self, inputs, state, seq_lens):
                model_out, h, c = self.rnn_model([inputs, seq_lens] + state)
                return model_out, [h, c]
        """
        raise NotImplementedError("You must implement this for a RNN model")

    def get_initial_state(self) -> List[TensorType]:
        """Get the initial recurrent state values for the model.

        Returns:
            list of np.array objects, if any

        Sample implementation for the ``MyRNNClass`` example::

            def get_initial_state(self):
                return [
                    np.zeros(self.cell_size, np.float32),
                    np.zeros(self.cell_size, np.float32),
                ]
        """
        raise NotImplementedError("You must implement this for a RNN model")

    @override(ModelV2)
    def preprocess_train_batch(self, train_batch):
        assert "state_in_0" in train_batch
        state_keys = []
        feature_keys_ = []
        for k, v in train_batch.items():
            if k.startswith("state_in_"):
                state_keys.append(k)
            elif not k.startswith("state_out_") and k != "infos" and isinstance(v, np.ndarray):
                feature_keys_.append(k)
    
        feature_sequences, initial_states, seq_lens = \
            chop_into_sequences(
                episode_ids=train_batch[SampleBatch.EPS_ID],
                unroll_ids=train_batch[SampleBatch.UNROLL_ID],
                agent_indices=train_batch[SampleBatch.AGENT_INDEX],
                feature_columns=[train_batch[k] for k in feature_keys_],
                state_columns=[train_batch[k] for k in state_keys],
                max_seq_len=self.model_config["max_seq_len"],
                dynamic_max=True,
                shuffle=False)
        for i, k in enumerate(feature_keys_):
            train_batch[k] = feature_sequences[i]
        for i, k in enumerate(state_keys):
            train_batch[k] = initial_states[i]
        train_batch["seq_lens"] = seq_lens
        return train_batch
    

class LSTMWrapper(RecurrentNetwork):
    """An LSTM wrapper serving as an interface for ModelV2s that set use_lstm.
    """

    def __init__(self, obs_space: gym.spaces.Space,
                 action_space: gym.spaces.Space, num_outputs: int,
                 model_config: ModelConfigDict, name: str):

        super(LSTMWrapper, self).__init__(obs_space, action_space, None,
                                          model_config, name)

        self.cell_size = model_config["lstm_cell_size"]
        self.use_prev_action_reward = model_config[
            "lstm_use_prev_action_reward"]
        if action_space.shape is not None:
            self.action_dim = int(np.product(action_space.shape))
        else:
            self.action_dim = int(len(action_space))
        # Add prev-action/reward nodes to input to LSTM.
        if self.use_prev_action_reward:
            self.num_outputs += 1 + self.action_dim

        # Define input layers.
        input_layer = tf.keras.layers.Input(
            shape=(None, self.num_outputs), name="inputs")

        self.num_outputs = num_outputs

        state_in_h = tf.keras.layers.Input(shape=(self.cell_size, ), name="h")
        state_in_c = tf.keras.layers.Input(shape=(self.cell_size, ), name="c")
        seq_in = tf.keras.layers.Input(shape=(), name="seq_in", dtype=tf.int32)

        # Preprocess observation with a hidden layer and send to LSTM cell
        lstm_out, state_h, state_c = tf.keras.layers.LSTM(
            self.cell_size,
            return_sequences=True,
            return_state=True,
            name="lstm")(
                inputs=input_layer,
                mask=tf.sequence_mask(seq_in),
                initial_state=[state_in_h, state_in_c])

        # Postprocess LSTM output with another hidden layer and compute values
        logits = tf.keras.layers.Dense(
            self.num_outputs,
            activation=tf.keras.activations.linear,
            name="logits")(lstm_out)
        values = tf.keras.layers.Dense(
            1, activation=None, name="values")(lstm_out)

        # Create the RNN model
        self._rnn_model = tf.keras.Model(
            inputs=[input_layer, seq_in, state_in_h, state_in_c],
            outputs=[logits, values, state_h, state_c])
        self.register_variables(self._rnn_model.variables)
        self._rnn_model.summary()

        # Add prev-a/r to this model's view, if required.
        if model_config["lstm_use_prev_action_reward"]:
            self.inference_view_requirements[SampleBatch.PREV_REWARDS] = \
                ViewRequirement(SampleBatch.REWARDS, data_rel_pos=-1)
            self.inference_view_requirements[SampleBatch.PREV_ACTIONS] = \
                ViewRequirement(SampleBatch.ACTIONS, space=self.action_space,
                                data_rel_pos=-1)

    @override(RecurrentNetwork)
    def forward(self, input_dict: Dict[str, TensorType],
                state: List[TensorType],
                seq_lens: TensorType) -> (TensorType, List[TensorType]):
        assert seq_lens is not None
        # Push obs through "unwrapped" net's `forward()` first.
        wrapped_out, _ = self._wrapped_forward(input_dict, [], None)

        # Concat. prev-action/reward if required.
        if self.model_config["lstm_use_prev_action_reward"]:
            wrapped_out = tf.concat(
                [
                    wrapped_out,
                    tf.reshape(
                        tf.cast(input_dict[SampleBatch.PREV_ACTIONS],
                                tf.float32), [-1, self.action_dim]),
                    tf.reshape(
                        tf.cast(input_dict[SampleBatch.PREV_REWARDS],
                                tf.float32), [-1, 1]),
                ],
                axis=1)

        # Then through our LSTM.
        input_dict["obs_flat"] = wrapped_out
        return super().forward(input_dict, state, seq_lens)

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs: TensorType, state: List[TensorType],
                    seq_lens: TensorType) -> (TensorType, List[TensorType]):
        model_out, self._value_out, h, c = self._rnn_model([inputs, seq_lens] +
                                                           state)
        return model_out, [h, c]

    @override(ModelV2)
    def get_initial_state(self) -> List[np.ndarray]:
        return [
            np.zeros(self.cell_size, np.float32),
            np.zeros(self.cell_size, np.float32),
        ]

    @override(ModelV2)
    def value_function(self) -> TensorType:
        return tf.reshape(self._value_out, [-1])
