from typing import Iterable

import pandas as pd


class _SourceShard:
    def prefix(self) -> str:
        raise NotImplementedError

    @property
    def shard_id(self) -> int:
        raise NotImplementedError

    def __iter__(self) -> Iterable[pd.DataFrame]:
        raise NotImplementedError

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return f"{self.prefix()}SourceShard[{self.shard_id}]"


class TensorDataset:
    def set_num_shards(self, num_shards, **kwargs):
        raise NotImplementedError

    def get_shard(self, shard_index: int, **kwargs):
        raise NotImplementedError
