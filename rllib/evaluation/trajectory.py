import logging
import numpy as np
from typing import Optional

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy

tf = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


@PublicAPI
class Trajectory:
    """A trajectory of a (single) agent throughout one episode.

    Collects all data produced by the environment during stepping of the agent
    as well as all model outputs associated with the agent's Policy.
    NOTE: A Trajectory object may contain remainders of a previous trajectory,
    however, these are only kept for avoiding memory re-allocations. A
    convenience cursor and offset allow for only "viewing" the currently
    ongoing trajectory.
    Pre-allocation happens over a given `horizon` range of timesteps. `horizon`
    may be float("inf"), in which case, we will allocate for some fixed
    n timesteps and double (re-allocate) the buffers each time this limit is
    reached.
    """

    # Disambiguate unrolls within a single episode.
    _next_unroll_id = 0

    @PublicAPI
    def __init__(self, buffer_size: Optional[int] = None):
        """Initializes a Trajectory object.

        Args:
            buffer_size (Optional[int]): The max number of timesteps to
                fit into one buffer column.
        """
        self.env_id = None
        self.agent_id = None
        self.policy_id = None

        # Determine the size of the initial buffers.
        self.buffer_size = buffer_size or 1000
        self.buffers = {}

        self.has_initial_obs: bool = False

        # Cursor into the preallocated buffers. This is where all new data
        # gets inserted.
        self.cursor: int = 0
        # The offset inside our buffer where the current trajectory starts.
        self.trajectory_offset: int = 0
        # The offset inside our buffer, from where to build the next
        # SampleBatch.
        self.sample_batch_offset: int = 0

    @property
    def timestep(self):
        # The timestep in the (currently ongoing) trajectory.
        return self.cursor - self.trajectory_offset

    @PublicAPI
    def add_init_obs(self, env_id, agent_id, policy_id, init_obs):
        """Adds a single initial observation (after env.reset()) to the buffer.

        Args:
            env_id (str): The env's ID for which we want to store the initial
                observation.
            agent_id (str): The agent's ID whose observation we want to store.
            init_obs (any): Initial observation (after env.reset()).
        """
        # Our buffer should be empty when we add the first observation.
        if SampleBatch.OBS not in self.buffers:
            assert self.has_initial_obs is False
            assert self.cursor == self.sample_batch_offset == \
                self.trajectory_offset == 0
            self.has_initial_obs = True
            # Build the buffer only for "obs" (needs +1 time step slot for the
            # last observation). Only increase `self.timestep` once we get the
            # other-than-obs data (which will include the next obs).
            obs_buffer = np.zeros(
                shape=(self.buffer_size + 1, ) + init_obs.shape,
                dtype=init_obs.dtype)
            obs_buffer[0] = init_obs
            self.buffers[SampleBatch.OBS] = obs_buffer
        else:
            assert self.has_initial_obs
            self.buffers[SampleBatch.OBS][self.cursor] = init_obs

        self.env_id = env_id
        self.agent_id = agent_id
        self.policy_id = policy_id

    @PublicAPI
    def add_action_reward_next_obs(self, env_id, agent_id, policy_id, values):
        """Add the given dictionary (row) of values to this batch.

        Args:
            values (Dict[str,any]): Data dict (interpreted as a single row)
                to be added to buffer. Must contain keys: SampleBatch.ACTIONS,
                REWARDS, DONES, and OBS.
        """
        assert self.has_initial_obs is True
        assert (SampleBatch.ACTIONS in values and SampleBatch.REWARDS in values
                and SampleBatch.NEXT_OBS in values)
        assert env_id == self.env_id
        assert agent_id == self.agent_id
        assert policy_id == self.policy_id

        # Only obs exists so far in buffers:
        # Initialize all other columns.
        if len(self.buffers) == 1:
            assert SampleBatch.OBS in self.buffers
            self._build_buffers(single_row=values)

        for k, v in values.items():
            if k == SampleBatch.NEXT_OBS:
                t = self.cursor + 1
                k = SampleBatch.OBS
            else:
                t = self.cursor
            self.buffers[k][t] = v
        self.cursor += 1

        # Extend (re-alloc) buffers if full.
        if self.cursor == self.buffer_size:
            self._extend_buffers(values)

    @PublicAPI
    def get_sample_batch_and_reset(self) -> SampleBatch:
        """Returns a SampleBatch carrying all previously added data.

        If a reset happens and the trajectory is not done yet, we'll keep the
        entire ongoing trajectory in memory for Model view requirement purposes
        and only actually free the data, once the episode ends.

        Returns:
            SampleBatch: The SampleBatch containing this agent's data for the
                entire trajectory (so far). The trajectory may not be
                terminated yet.
        """

        # Convert all our data to numpy arrays, compress float64 to float32,
        # and add the last observation data as well (always one more obs than
        # all other columns due to the additional obs returned by Env.reset()).
        data = {}
        for k, v in self.buffers.items():
            data[k] = convert_to_numpy(
                v[self.sample_batch_offset:self.cursor], reduce_floats=True)
        last_obs = {
            self.agent_id: convert_to_numpy(
                self.buffers[SampleBatch.OBS][self.cursor], reduce_floats=True)
        }
        batch = SampleBatch(data, _last_obs=last_obs)

        # Add unroll ID column to batch if non-existent.
        if SampleBatch.UNROLL_ID not in batch.data:
            batch.data[SampleBatch.UNROLL_ID] = np.repeat(
                Trajectory._next_unroll_id, batch.count)
            Trajectory._next_unroll_id += 1

        # If done at end -> We can reset our buffers entirely.
        if self.buffers[SampleBatch.DONES][self.cursor - 1]:
            # Set self.timestep to 0 -> new trajectory w/o re-alloc (not yet,
            # only ever re-alloc when necessary).
            self.trajectory_offset = self.sample_batch_offset = self.cursor
        # No done at end -> leave trajectory_offset as is (trajectory is still
        # ongoing), but move the sample_batch offset to cursor.
        else:
            self.sample_batch_offset = self.cursor
        return batch

    def _build_buffers(self, single_row):
        """Creates zero-filled pre-allocated numpy buffers for data collection.

        Except for the obs-column, which should already be initialized (done
        on call to `self.add_initial_observation()`).

        Args:
            single_row (Dict[str,np.ndarray]): Dict of column names (keys) and
                sample numpy data (values). Note: Only one of `single_data` or
                `data_batch` must be provided.
        """
        for col, data in single_row.items():
            if col == SampleBatch.NEXT_OBS:
                assert SampleBatch.OBS not in single_row
                col = SampleBatch.OBS
            # Skip already initialized ones, e.g. 'obs' if used with
            # add_initial_observation.
            if col in self.buffers:
                continue
            next_obs_add = 1 if col == SampleBatch.OBS else 0
            # Primitive.
            if isinstance(data, (int, float, bool)):
                shape = (self.buffer_size + next_obs_add, )
                t_ = type(data)
                dtype = np.float32 if t_ == float else \
                    np.int32 if type(data) == int else np.bool_
                self.buffers[col] = np.zeros(shape=shape, dtype=dtype)
            # np.ndarray, torch.Tensor, or tf.Tensor.
            else:
                shape = (self.buffer_size + next_obs_add,) + \
                        data.shape
                dtype = data.dtype
                if torch and isinstance(data, torch.Tensor):
                    self.buffers[col] = torch.zeros(
                        *shape, dtype=dtype, device=data.device)
                elif tf and isinstance(data, tf.Tensor):
                    self.buffers[col] = tf.zeros(shape=shape, dtype=dtype)
                else:
                    self.buffers[col] = np.zeros(shape=shape, dtype=dtype)

    def _extend_buffers(self, single_row):
        traj_length = self.cursor - self.trajectory_offset
        # Trajectory starts at 0 (meaning episodes are longer than current
        # `self.buffer_size` -> Simply do a resize (enlarge) on each column
        # in the buffer.
        if self.trajectory_offset == 0:
            # Double actual horizon.
            self.buffer_size *= 2
            for col, data in self.buffers.items():
                data.resize((self.buffer_size, ) + data.shape[1:])
        # Trajectory starts in first half of the buffer -> Reallocate a new
        # buffer and copy the currently ongoing trajectory into the new buffer.
        elif self.trajectory_offset < self.buffer_size / 2:
            # Double actual horizon.
            self.buffer_size *= 2
            # Store currently ongoing trajectory and build a new buffer.
            old_buffers = self.buffers
            self.buffers = {}
            self._build_buffers(single_row)
            # Copy the still ongoing trajectory into the new buffer.
            for col, data in old_buffers.items():
                self.buffers[col][:traj_length] = data[self.trajectory_offset:
                                                       self.cursor]
        # Do an efficient memory swap: Move current trajectory simply to
        # the beginning of the buffer (no reallocation/zero-padding necessary).
        else:
            for col, data in self.buffers.items():
                self.buffers[col][:traj_length] = self.buffers[col][
                    self.trajectory_offset:self.cursor]

        # Set all pointers to their correct new values.
        self.sample_batch_offset = (
            self.sample_batch_offset - self.trajectory_offset)
        self.trajectory_offset = 0
        self.cursor = traj_length
