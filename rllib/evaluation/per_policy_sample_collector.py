import logging
import numpy as np
from typing import Dict, Optional

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.types import AgentID, EnvID, EpisodeID, TensorType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


class PerPolicySampleCollector:
    """
    """

    def __init__(self,
                 num_agents: Optional[int] = None,
                 num_timesteps: Optional[int] = None,
                 time_major: bool = True,
                 shift_before=0,
                 shift_after=0,
                 policy_id=None):
        """Initializes a ... object.

        Args:
        """
        self.policy_id = policy_id
        self.num_agents = num_agents or 100
        self.num_timesteps = num_timesteps
        self.time_major = time_major
        # `shift_before must at least be 1 for the init obs timestep.
        self.shift_before = max(shift_before, 1)
        self.shift_after = shift_after

        # The offset on the agent dim to start the next SampleBatch build from.
        self.sample_batch_offset = 0

        self.buffers = {}
        self.postprocessed_agents = [False] * self.num_agents

        # Next agent-slot to be used by a new agent/env combination.
        self.agent_slot_cursor = 0
        # Maps agent/episode ID/chunk-num to an agent slot.
        self.agent_key_to_slot = {}
        # Maps agent/episode ID to the last chunk-num.
        self.agent_key_to_chunk_num = {}
        # Maps agent slot number to agent keys.
        self.slot_to_agent_key = [None] * self.num_agents
        # Maps agent/episode ID/chunk-num to a time step cursor.
        self.agent_key_to_timestep = {}

        # Total timesteps taken in the env over all agents since last reset.
        self.timesteps_since_last_reset = 0

        # Indices (T,B) to pick from the buffers for the next forward pass.
        self.forward_pass_indices = [[], []]
        self.forward_pass_size = 0
        # Maps index from the forward pass batch to (agent_id, episode_id,
        # env_id) tuple.
        self.forward_pass_index_to_agent_info = {}
        self.agent_key_to_forward_pass_index = {}

    def add_init_obs(self, episode_id: EpisodeID, agent_id: AgentID,
                     env_id: int, chunk_num: int, init_obs: TensorType) -> None:
        """Adds a single initial observation (after env.reset()) to the buffer.

        #Stores it in self.initial_obs.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are adding the
                initial observation for.
            agent_id (AgentID): Unique id for the agent we are adding the
                initial observation for.
            init_obs (TensorType): Initial observation (after env.reset()).
        """
        agent_key = (agent_id, episode_id, chunk_num)
        agent_slot = self.agent_slot_cursor
        self.agent_key_to_slot[agent_key] = agent_slot
        self.agent_key_to_chunk_num[agent_key[:2]] = chunk_num
        self.slot_to_agent_key[agent_slot] = agent_key
        self.next_agent_slot()

        if SampleBatch.OBS not in self.buffers:
            self._build_buffers(single_row={SampleBatch.OBS: init_obs})
        if self.time_major:
            self.buffers[SampleBatch.OBS][self.shift_before-1, agent_slot] = \
                init_obs
        else:
            self.buffers[SampleBatch.OBS][agent_slot, self.shift_before-1] = \
                init_obs
        self.agent_key_to_timestep[agent_key] = self.shift_before

        self._add_to_next_inference_call(
            agent_key, env_id, agent_slot, self.shift_before-1)

    def add_action_reward_next_obs(self, episode_id: EpisodeID,
                                   agent_id: AgentID,
                                   env_id: EnvID,
                                   agent_done: bool,
                                   values: Dict[str, TensorType]) -> None:
        """Add the given dictionary (row) of values to this batch.

        Args:
            episode_id (EpisodeID): Unique id for the episode we are adding the
                values for.
            agent_id (AgentID): Unique id for the agent we are adding the
                values for.
            agent_done (bool): Whether next obs should not be used for an
                upcoming inference call. Default: False = next-obs should be
                used for upcoming inference.
            values (Dict[str, TensorType]): Data dict (interpreted as a single
                row) to be added to buffer. Must contain keys:
                SampleBatch.ACTIONS, REWARDS, DONES, and NEXT_OBS.
        """
        assert (SampleBatch.ACTIONS in values and SampleBatch.REWARDS in values
                and SampleBatch.NEXT_OBS in values
                and SampleBatch.DONES in values)

        assert SampleBatch.OBS not in values
        values[SampleBatch.OBS] = values[SampleBatch.NEXT_OBS]
        del values[SampleBatch.NEXT_OBS]

        chunk_num = self.agent_key_to_chunk_num[(agent_id, episode_id)]
        agent_key = (agent_id, episode_id, chunk_num)
        agent_slot = self.agent_key_to_slot[agent_key]
        ts = self.agent_key_to_timestep[agent_key]
        for k, v in values.items():
            if k not in self.buffers:
                self._build_buffers(single_row=values)
            if self.time_major:
                self.buffers[k][ts, agent_slot] = v
            else:
                self.buffers[k][agent_slot, ts] = v
        self.agent_key_to_timestep[agent_key] += 1

        # Time-axis is "full" -> Cut-over to new chunk (only if not DONE).
        if self.agent_key_to_timestep[
            agent_key] - self.shift_before == self.num_timesteps and \
                not values[SampleBatch.DONES]:
            self.new_chunk_from(agent_slot, agent_key,
                                self.agent_key_to_timestep[agent_key])

        self.timesteps_since_last_reset += 1

        if not agent_done:
            self._add_to_next_inference_call(
                agent_key, env_id, agent_slot, ts)

    def next_agent_slot(self):
        self.agent_slot_cursor += 1
        if self.agent_slot_cursor >= self.num_agents:
            self.agent_slot_cursor = 0
        # Just make sure, there is space in our buffer.
        assert self.slot_to_agent_key[self.agent_slot_cursor] is None

    def new_chunk_from(self, agent_slot, agent_key, timestep):
        new_agent_slot = self.agent_slot_cursor
        new_agent_key = agent_key[:2] + (agent_key[2] + 1, )
        # Copy everything from agent_slot into new_slot.
        if self.time_major:
            for k in self.buffers.keys():
                self.buffers[k][0:self.shift_before, new_agent_slot] = \
                    self.buffers[k][timestep - self.shift_before:timestep,
                    agent_slot]
        else:
            for k in self.buffers.keys():
                self.buffers[k][new_agent_slot, 0:self.shift_before] = \
                    self.buffers[k][agent_slot,
                    timestep - self.shift_before:timestep]

        self.agent_key_to_slot[new_agent_key] = new_agent_slot
        self.agent_key_to_chunk_num[new_agent_key[:2]] = new_agent_key[2]
        self.slot_to_agent_key[new_agent_slot] = new_agent_key
        self.next_agent_slot()
        self.agent_key_to_timestep[new_agent_key] = self.shift_before

    def _add_to_next_inference_call(
            self, agent_key, env_id, agent_slot, timestep):
        #agent_key = (agent_id, episode_id,
        #             self.agent_key_to_chunk_num[(agent_id, episode_id)])
        #agent_slot = self.agent_key_to_slot[agent_key]
        #timestep = self.agent_key_to_timestep[agent_key]
        idx = self.forward_pass_size
        self.forward_pass_index_to_agent_info[idx] = (
            agent_key[0], agent_key[1], env_id)
        self.agent_key_to_forward_pass_index[agent_key[:2]] = idx
        if self.forward_pass_size == 0:
            self.forward_pass_indices[0].clear()
            self.forward_pass_indices[1].clear()
        self.forward_pass_indices[0].append(timestep)
        self.forward_pass_indices[1].append(agent_slot)
        self.forward_pass_size += 1

    def _reset_inference_call(self):
        self.forward_pass_size = 0

    def get_train_sample_batch_and_reset(self, view_reqs) -> SampleBatch:
        """Returns a SampleBatch carrying all previously added data.

        If a reset happens and the trajectory is not done yet, we'll keep the
        entire ongoing trajectory in memory for Model view requirement purposes
        and only actually free the data, once the episode ends.

        Args:
            #model (ModelV2): The ModelV2 object for which to generate the view
            #    (input_dict) the buffers.

        Returns:
            SampleBatch: A SampleBatch containing data for training the Policy.
        """
        # Get ModelV2's view requirements.
        #view_reqs = model.get_view_requirements(is_training=True)

        # Construct the view dict.
        view = {}
        for view_col, view_req in view_reqs.items():
            # Skip columns that do not need to be included for training.
            #if not view_req.training:
            #    continue
            data_col = view_req.data_col or view_col
            assert data_col in self.buffers
            extra_shift = 0
            # For OBS, indices must be shifted by -1.
            if data_col == SampleBatch.OBS:
                extra_shift = -1
            t_start = self.shift_before + extra_shift
            t_end = t_start + self.num_timesteps
            # If agent_slot has been rolled-over to beginning, we have to copy
            # here.
            if self.agent_slot_cursor < self.sample_batch_offset:
                time_slice = self.buffers[data_col][t_start:t_end]
                one_ = time_slice[:, self.sample_batch_offset:]
                two_ = time_slice[:, :self.agent_slot_cursor]
                if torch and isinstance(time_slice, torch.Tensor):
                    view[view_col] = torch.cat([one_, two_], dim=1)
                else:
                    view[view_col] = np.concatenate([one_, two_], axis=1)
            else:
                view[view_col] = \
                    self.buffers[data_col][
                    t_start:t_end,
                    self.sample_batch_offset:self.agent_slot_cursor]

        seq_lens = [
            self.agent_key_to_timestep[k] - 1 for k in self.slot_to_agent_key
            if k is not None
        ]
        batch = SampleBatch(
            view, _seq_lens=np.array(seq_lens), _time_major=True)

        call_args = []

        # Copy all still ongoing trajectories to new agent slots.
        for i, seq_len in enumerate(seq_lens):
            if seq_len < self.num_timesteps:
                agent_slot = self.sample_batch_offset + i
                if agent_slot >= self.num_agents:
                    agent_slot = agent_slot % self.num_agents
                if not self.buffers[SampleBatch.
                                    DONES][seq_len - 1 +
                                           self.shift_before][agent_slot]:
                    agent_key = self.slot_to_agent_key[agent_slot]
                    call_args.append((agent_slot, agent_key,
                                      self.agent_key_to_timestep[agent_key]))

        # Reset everything for new data.
        self.postprocessed_agents = [False] * self.num_agents
        self.agent_key_to_slot.clear()
        self.agent_key_to_chunk_num.clear()
        self.slot_to_agent_key = [None] * self.num_agents
        self.agent_key_to_timestep.clear()
        self.timesteps_since_last_reset = 0
        self.forward_pass_size = 0
        self.sample_batch_offset = self.agent_slot_cursor

        for args in call_args:
            self.new_chunk_from(*args)

        return batch

    def get_inference_input_dict(self, view_reqs) -> Dict[str, TensorType]:
        """Returns an input_dict for a Model's forward pass given our data.

        Args:
            #model (ModelV2): The ModelV2 object for which to generate the view
            #    (input_dict) from `data`.
            #is_training (bool): Whether the view should be generated for
            #    training purposes or inference (default).

        Returns:
            Dict[str, TensorType]: The input_dict to be passed into the ModelV2
                for inference/training.
        """
        # Construct the view dict.
        input_dict = {}
        for view_col, view_req in view_reqs.items():
            # Create the batch of data from the different buffers.
            data_col = view_req.data_col or view_col
            if data_col not in self.buffers:
                self._build_buffers({data_col: view_req.space.sample()})

            # For OBS, indices must be shifted by -1.
            #if data_col == SampleBatch.OBS:
            #    t = self.forward_pass_indices[0]
            #    indices = (list(np.array(t) - 1), self.forward_pass_indices[1])
            #else:
            indices = self.forward_pass_indices
            if self.time_major:
                input_dict[view_col] = self.buffers[data_col][indices]
            else:
                if isinstance(view_req.shift, (list, tuple)):
                    time_indices = np.array(view_req.shift) + np.array(indices[0])
                    input_dict[view_col] = self.buffers[data_col][
                        indices[1], time_indices]
                else:
                    input_dict[view_col] = self.buffers[data_col][indices[1], indices[0]]

        self._reset_inference_call()

        return input_dict

    def get_postprocessing_sample_batches(self, episode, view_reqs):
        # Loop through all agents and create a SampleBatch
        # (as "view"; no copying).

        # Construct the SampleBatch-dict.
        sample_batch_data = {}

        range_ = self.agent_slot_cursor - self.sample_batch_offset
        if range_ < 0:
            range_ = self.num_agents + range_
        for i in range(range_):
            agent_slot = self.sample_batch_offset + i
            if agent_slot >= self.num_agents:
                agent_slot = agent_slot % self.num_agents
            # Do not postprocess the same slot twice.
            if self.postprocessed_agents[agent_slot]:
                continue
            agent_key = self.slot_to_agent_key[agent_slot]
            # Skip other episodes (if episode provided).
            if episode and agent_key[1] != episode.episode_id:
                continue
            end = self.agent_key_to_timestep[agent_key]
            # Do not build any empty SampleBatches.
            if end == self.shift_before:
                continue
            self.postprocessed_agents[agent_slot] = True

            assert agent_key not in sample_batch_data
            sample_batch_data[agent_key] = {}
            batch = sample_batch_data[agent_key]

            for view_col, view_req in view_reqs.items():
                # Skip columns that will only get added through postprocessing
                # (these may not even exist yet).
                if view_req.created_during_postprocessing:
                    continue

                data_col = view_req.data_col or view_col
                shift = view_req.shift
                if data_col == SampleBatch.OBS:
                    shift -= 1

                batch[view_col] = self.buffers[data_col][
                    self.shift_before + shift:end + shift, agent_slot]

        batches = {}
        for agent_key, data in sample_batch_data.items():
            batches[agent_key] = SampleBatch(data)
        return batches

    def _build_buffers(self, single_row) -> None:
        """
        Args:
        """
        time_size = self.num_timesteps + self.shift_before + self.shift_after
        for col, data in single_row.items():
            if col in self.buffers:
                continue
            base_shape = (time_size, self.num_agents) if self.time_major else \
                (self.num_agents, time_size)
            # Python primitive -> np.array.
            if isinstance(data, (int, float, bool)):
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=base_shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = base_shape + data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _extend_buffers(self, sample_batch):
        """Extends the buffers on the batch dimension.

        Args:
            sample_batch (SampleBatch): SampleBatch to determine sizes and
                dtypes of the data columns to be preallocated (zero-filled)
                in case of a new (larger) buffer creation.
        """
        raise NotImplementedError
        sample_batch_size = self.cursor - self.sample_batch_offset
        # SampleBatch to-be-built-next starts in first half of the buffer ->
        # Reallocate a new buffer and copy the currently ongoing SampleBatch
        # into the new buffer.
        if self.sample_batch_offset < self.buffer_size / 2:
            # Double actual horizon.
            self.buffer_size *= 2
            # Store currently ongoing trajectory and build a new buffer.
            old_buffers = self.buffers
            self.buffers = {}
            self._build_buffers(sample_batch)
            # Copy the still ongoing trajectory into the new buffer.
            for col, data in old_buffers.items():
                self.buffers[col][:sample_batch_size] = \
                    data[self.sample_batch_offset:self.cursor]
        # Do an efficient memory swap: Move current SampleBatch
        # to-be-built-next simply to the beginning of the buffer
        # (no reallocation/zero-padding necessary).
        else:
            for col, data in self.buffers.items():
                self.buffers[col][:sample_batch_size] = self.buffers[col][
                    self.sample_batch_offset:self.cursor]

        # Set all pointers to their correct new values.
        self.sample_batch_offset = 0
        self.cursor = sample_batch_size
