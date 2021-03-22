import torch
import gym
from torch import nn
from typing import Union, Dict, List, Tuple
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.typing import ModelConfigDict, TensorType

try:
    from dnc import DNC
except ModuleNotFoundError:
    print("dnc module not found. Did you forget to 'pip install dnc'?")
    raise


class DNCMemory(TorchModelV2, nn.Module):
    """Differentiable Neural Computer wrapper around ixaxaar's DNC implementation,
    see https://github.com/ixaxaar/pytorch-dnc"""

    DEFAULT_CONFIG = {
        "dnc_model": DNC,
        "hidden_size": 128,
        "num_layers": 1,
        "num_hidden_layers": 2,
        "read_heads": 4,
        "nr_cells": 32,
        "cell_size": 16,
    }

    MEMORY_KEYS = [
        "memory",
        "link_matrix",
        "precedence",
        "read_weights",
        "write_weights",
        "usage_vector",
    ]

    def __init__(
            self,
            obs_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            num_outputs: int,
            model_config: ModelConfigDict,
            name: str,
            **custom_model_kwargs,
    ):
        nn.Module.__init__(self)
        super(DNCMemory, self).__init__(obs_space, action_space, num_outputs,
                                        model_config, name)
        self.num_outputs = num_outputs
        self.obs_dim = gym.spaces.utils.flatdim(obs_space)
        self.act_dim = gym.spaces.utils.flatdim(action_space)

        self.cfg = dict(self.DEFAULT_CONFIG, **custom_model_kwargs)
        self.cur_val = None

        self.logit_branch = SlimFC(
            in_size=self.obs_dim,
            out_size=self.num_outputs,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )

        self.value_branch = SlimFC(
            in_size=self.obs_dim,
            out_size=1,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_,
        )

        self.dnc: Union[None, DNC] = None

    def get_initial_state(self) -> List[TensorType]:
        ctrl_hidden = [
            torch.zeros(2, self.cfg["hidden_size"]),
            torch.zeros(2, self.cfg["hidden_size"]),
        ]
        m = self.cfg["nr_cells"]
        r = self.cfg["read_heads"]
        w = self.cfg["cell_size"]
        memory = [
            torch.zeros(m, w),  # memory
            torch.zeros(1, m, m),  # link_matrix
            torch.zeros(1, m),  # precedence
            torch.zeros(r, m),  # read_weights
            torch.zeros(1, m),  # write_weights
            torch.zeros(m),  # usage_vector
        ]

        read_vecs = torch.zeros(w * r)

        state = [*ctrl_hidden, read_vecs, *memory]
        assert len(state) == 9
        return state

    def value_function(self) -> TensorType:
        assert self.cur_val is not None, "must call forward() first"
        return self.cur_val

    def unpack_state(self, state: List[TensorType], 
                     ) -> Tuple[List[Tuple[TensorType, TensorType]],
                                      Dict[str, TensorType], TensorType]:
        """Given a list of tensors, reformat for self.dnc input"""
        assert len(state) == 9, "Failed to verify unpacked state"
        ctrl_hidden: List[Tuple[TensorType, TensorType]] = [(
            state[0].permute(1, 0, 2).contiguous(),
            state[1].permute(1, 0, 2).contiguous(),
        )]
        read_vecs: TensorType = state[2]
        memory: List[TensorType] = state[3:]
        memory_dict: Dict[str, TensorType] = dict(
            zip(self.MEMORY_KEYS, memory))

        return ctrl_hidden, memory_dict, read_vecs

    def pack_state(
            self,
            ctrl_hidden: List[Tuple[TensorType, TensorType]],
            memory_dict: Dict[str, TensorType],
            read_vecs: TensorType,
    ) -> List[TensorType]:
        """Given the dnc output, pack it into a list of tensors
        for rllib state. Order is ctrl_hidden, read_vecs, memory_dict"""
        state = []
        ctrl_hidden = [
            ctrl_hidden[0][0].permute(1, 0, 2),
            ctrl_hidden[0][1].permute(1, 0, 2),
        ]
        state += ctrl_hidden  # len 2
        state.append(read_vecs)  # len 3
        state += memory_dict.values()  # len 9
        assert len(state) == 9, "Failed to verify packed state"
        return state

    def validate_unpack(self, dnc_output, unpacked_state):
        """Ensure the unpacked state shapes match the DNC output"""
        s_ctrl_hidden, s_memory_dict, s_read_vecs = unpacked_state
        ctrl_hidden, memory_dict, read_vecs = dnc_output

        for i in range(len(ctrl_hidden)):
            for j in range(len(ctrl_hidden[i])):
                assert s_ctrl_hidden[i][j].shape == ctrl_hidden[i][j].shape, (
                    "Controller state mismatch: got "
                    f"{s_ctrl_hidden[i][j].shape} should be "
                    f"{ctrl_hidden[i][j].shape}")

        for k in memory_dict:
            assert s_memory_dict[k].shape == memory_dict[k].shape, (
                "Memory state mismatch at key "
                f"{k}: got {s_memory_dict[k].shape} should be "
                f"{memory_dict[k].shape}")

        assert s_read_vecs.shape == read_vecs.shape, (
            "Read state mismatch: got "
            f"{s_read_vecs.shape} should be "
            f"{read_vecs.shape}")

    def forward(
            self,
            input_dict: Dict[str, TensorType],
            state: List[TensorType],
            seq_lens: TensorType,
    ) -> Tuple[TensorType, List[TensorType]]:

        flat = input_dict["obs_flat"]
        # Batch and Time
        # Forward expects outputs as [B, T, logits]
        B = len(seq_lens)
        T = flat.shape[0] // B

        logits = torch.zeros(B, T, self.num_outputs, device=flat.device)
        values = torch.zeros(B, T, 1, device=flat.device)
        # Deconstruct batch into batch and time dimensions: [B, T, feats]
        flat = torch.reshape(flat, [-1, T] + list(flat.shape[1:]))

        # First run
        if self.dnc is None:
            (ctrl_hidden, read_vecs, memory_dict) = (None, None, None)
            gpu_id = flat.device.index if flat.device.index is not None else -1
            self.dnc = self.cfg["dnc_model"](
                input_size=self.obs_dim,
                hidden_size=self.cfg["hidden_size"],
                num_layers=self.cfg["num_layers"],
                read_heads=self.cfg["read_heads"],
                cell_size=self.cfg["cell_size"],
                nr_cells=self.cfg["nr_cells"],
                gpu_id=gpu_id,
            )
            output, (ctrl_hidden, memory_dict, read_vecs) = self.dnc(
                flat, (ctrl_hidden, memory_dict, read_vecs))

        else:
            ctrl_hidden, memory_dict, read_vecs = self.unpack_state(state)
            output, (ctrl_hidden, memory_dict, read_vecs) = self.dnc(
                flat, (ctrl_hidden, memory_dict, read_vecs))

        packed_state = self.pack_state(ctrl_hidden, memory_dict, read_vecs)

        # Compute action/value from output
        for t in range(T):
            logits[:, t] = self.logit_branch(output[:, t])
            values[:, t] = self.value_branch(output[:, t])

        logits = logits.reshape((B * T, self.num_outputs))
        values = values.reshape((B * T, 1))

        self.cur_val = values.squeeze(1)

        return logits, packed_state
