"""
[1] - Attention Is All You Need - Vaswani, Jones, Shazeer, Parmar,
      Uszkoreit, Gomez, Kaiser - Google Brain/Research, U Toronto - 2017.
      https://arxiv.org/pdf/1706.03762.pdf
"""
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.torch_ops import sequence_mask


torch, nn = try_import_torch()


class MultiHeadAttention(nn.Module):
    """A multi-head attention layer described in [1]."""

    # Change to the tf implementation: include the in_dim parameter
    def __init__(self, in_dim,  out_dim, num_heads, head_dim, **kwargs):
        super().__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = SlimFC(
            in_size=in_dim,
            out_size=3 * num_heads * head_dim,
            use_bias=False)

        #TODO port the keras.layers.TimeDistributed wrapper
        self._linear_layer = SlimFC(
            in_size=3 * num_heads * head_dim,
            out_size=out_dim,
            use_bias=False)

    def forward(self, inputs):
        L = list(inputs.size())[1]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        qkv = self._qkv_layer(inputs)

        queries, keys, values = torch.chunk(input=qkv, chunks=3, dim=-1)
        queries = queries[:, -L:]  # only query based on the segment

        queries = torch.reshape(queries, [-1, L, H, D])
        keys = torch.reshape(keys, [-1, L, H, D])
        values = torch.reshape(values, [-1, L, H, D])

        score = torch.einsum("bihd,bjhd->bijh", queries, keys)
        score = score / D**0.5

        # causal mask of the same length as the sequence
        mask = sequence_mask(torch.range(1, L + 1), dtype=score.dtype)
        mask = mask[None, :, :, None]

        masked_score = score * mask + 1e30 * (mask - 1.)
        wmat = nn.Softmax(masked_score, dim=2)

        out = torch.einsum("bijh,bjhd->bihd", wmat, values)
        out = torch.reshape(out,
                            torch.concat((list(out.size())[:2], [H * D]),
                            dim=0))
        return self._linear_layer(out)

