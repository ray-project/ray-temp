from typing import Any, Callable, List, Iterable, Optional

import pandas as pd
from pandas import DataFrame

from ray.util.iter import _NextValueNotReady, LocalIterator, ParallelIterator, T
import ray.util.iter as parallel_iter

import random

from collections import Iterator


def is_list_like(item) -> bool:
    return isinstance(item, (list, tuple)) or hasattr(item, "__iter__")


class PandasDataset:
    def __init__(self, it: ParallelIterator[DataFrame]):
        super(PandasDataset, self).__init__(it.actor_sets, it.name,
                                            it.parent_iterators)
        self._base_it: ParallelIterator[DataFrame] = it

    @staticmethod
    def from_items(items: List[T], num_shards: int = 2,
                   repeat: bool = False) -> "PandasDataset":
        return PandasDataset.from_it(
            parallel_iter.from_items(items, num_shards, repeat))

    @staticmethod
    def from_range(n: int, num_shards: int = 2,
                   repeat: bool = False) -> "PandasDataset":
        return PandasDataset.from_it(
            parallel_iter.from_range(n, num_shards, repeat))

    @staticmethod
    def from_iterators(generators: List[Iterable[T]],
                       repeat: bool = False,
                       name=None) -> "PandasDataset":
        return PandasDataset.from_iterators(generators, repeat, name)

    @staticmethod
    def from_it(it: ParallelIterator,
                batch_size: int = 32) -> "PandasDataset":
        def fn(items: Iterable):
            try:
                items = iter(items)
                item = next(items)
                if is_list_like(item):
                    yield item
                    for item in items:
                        yield item
                else:
                    yield [item]
                    for item in items:
                        yield [item]
            except StopIteration:
                pass

        it = it.transform(fn)
        it = it.batch(batch_size)

        def to_pandas(items: Iterable) -> Iterable[DataFrame]:
            for item in items:
                yield pd.DataFrame(item)
        it = it.transform(to_pandas)
        return PandasDataset(it)

    def __iter__(self):
        raise Exception("Unsupported operation")

    def __repr__(self):
        return "PandasDataset[{}]".format(self.name)

    def _with_transform(self, local_it_fn, name):
        it = self._base_it._with_transform(local_it_fn, name)
        return PandasDataset(it)

    def transform(self,
                  fn: Callable[[Iterable[DataFrame]], Iterable[DataFrame]]
                  ) -> "PandasDataset":
        return PandasDataset(self._base_it.transform(fn))

    def repartition(self, num_partitions: int,
                    batch_ms: int = 0) -> "PandasDataset":
        it = self._base_it.repartition(num_partitions, batch_ms)
        return PandasDataset(it)

    def num_shards(self) -> int:
        """Return the number of worker actors backing this iterator."""
        return sum(len(a.actors) for a in self._base_it.actor_sets)

    def for_each(self,
                 fn: Callable[[DataFrame], DataFrame],
                 max_concurrency=1,
                 resources=None) -> "PandasDataset":
        it = self._base_it.for_each(fn, max_concurrency, resources)
        return PandasDataset(it)

    def batch(self, batch_size: int) -> "PandasDataset":
        """
        Unlike the ParallelIterator.batch. This method rebatch the underlying
        the pandas DataFrame, and each pandas DataFrame will have batch_size
        rows.
        """

        def batch_fn(it: Iterable[DataFrame]) -> Iterable[DataFrame]:
            it = iter(it)
            cur_df = None
            cur_index = 0
            cur_size = 0
            return_df = None
            while True:
                try:
                    cur_df = next(it)
                    while cur_df or (cur_index + batch_size) < cur_size:
                        if not cur_df or cur_index == cur_size:
                            cur_df = next(it)
                            cur_index = 0
                            cur_size = cur_df.shape[0]
                        if return_df:
                            ri = cur_index + batch_size - return_df.shape[0]
                            ri = min(ri, cur_size)
                            tmp = cur_df.iloc[cur_index, ri]
                            return_df = pd.concat([return_df, tmp])
                            cur_index = ri
                        else:
                            ri = cur_index + batch_size
                            ri = min(ri, cur_size)
                            return_df = cur_df.iloc[cur_index:ri]
                            cur_index = ri
                        if return_df.shape[0] == batch_size:
                            yield return_df
                            return_df = None
                except StopIteration:
                    break

            if return_df:
                return_df.index = range(return_df.shape[0])
                yield return_df

        return self._with_transform(
            lambda local_it: local_it.transform(batch_fn),
            f".batch({batch_size})")

    def local_shuffle(self, shuffle_buffer_size: int,
                      seed: int = None) -> "PandasDataset":
        it = self._base_it.local_shuffle(shuffle_buffer_size, seed)

        def shuffle_fn(it: Iterable[DataFrame]) -> Iterable[DataFrame]:
            for df in it:
                df = df.sample(frac=1, random_state=seed)
                yield df

        name = f".inner_pandas_shuffle(seed={seed})"
        it = it._with_transform(
            lambda local_it: local_it.transform(shuffle_fn), name)
        return PandasDataset(it)

    def get_shard(self,
                  shard_index: int,
                  batch_ms: int = 0,
                  num_async: int = 1,
                  shuffle: bool = False,
                  shuffle_buffer_size: int = 1,
                  seed: int = None) -> Iterator[DataFrame]:
        return _ShuffledIterator(self, shard_index, batch_ms, num_async,
                                 shuffle, shuffle_buffer_size, seed)

    def to_torch(self,
                 feature_columns: List[str] = None,
                 feature_shapes: Optional[List[Any]] = None,
                 feature_types: Optional[List["torch.dtype"]] = None,
                 label_column: str = None,
                 label_shape: Optional[int] = None,
                 label_type: Optional["torch.dtype"] = None) -> "TorchDataset":
        from ray.util.sgd.torch.torch_dataset import TorchDataset
        return TorchDataset(self, feature_columns, feature_shapes,
                            feature_types, label_column, label_shape,
                            label_type)

    def to_tf(self, feature_columns: List[str],
              feature_shapes: List["tensorflow.TensorShape"],
              feature_types: List["tensorflow.DType"], label_column: str,
              label_shape: "tensorflow.TensorShape",
              label_type: "tensorflow.DType"):
        from ray.util.sgd.tf.tf_dataset import TFDataset
        return TFDataset(self, feature_columns, feature_shapes, feature_types,
                         label_column, label_shape, label_type)


class _ShuffledIterator(Iterator[DataFrame]):
    def __init__(self,
                 it: ParallelIterator[DataFrame],
                 shard_index: int,
                 batch_ms: int = 0,
                 num_async: int = 1,
                 shuffle: bool = False,
                 shuffle_buffer_size: int = 1,
                 seed: int = None):
        super(_ShuffledIterator, self).__init__()
        self._it = it
        self._shard_index = shard_index
        self._batch_ms = batch_ms
        self._num_async = num_async
        self._shuffle = shuffle
        self._shuffle_buffer_size = shuffle_buffer_size
        self._seed = seed

        self._local_it: LocalIterator[DataFrame] = None

    def __next__(self) -> DataFrame:
        assert self._local_it is not None
        return next(self._local_it)

    def __iter__(self) -> Iterator[DataFrame]:
        it = self._it.get_shard(self._shard_index, self._batch_ms,
                                self._num_async)
        if self._shuffle:
            it = self.shuffle(it)

        self._local_it = it
        return self

    def shuffle(self, local_it: LocalIterator[DataFrame]
                ) -> LocalIterator[DataFrame]:
        shuffle_random = random.Random(self._seed)

        def apply_shuffle(it):
            buffer = []
            for item in it:
                if isinstance(item, _NextValueNotReady):
                    yield item
                else:
                    buffer.append(item)
                    if len(buffer) >= self._shuffle_buffer_size:
                        df = buffer.pop(
                            shuffle_random.randint(0,
                                                   len(buffer) - 1))
                        df = df.sample(frac=1, random_state=self._seed)
                        yield df
            while len(buffer) > 0:
                yield buffer.pop(shuffle_random.randint(0, len(buffer) - 1))

        return LocalIterator(
            local_it.base_iterator,
            local_it.shared_metrics,
            local_it.local_transforms + [apply_shuffle],
            name=local_it.name +
            ".shuffle(shuffle_buffer_size={}, seed={})".format(
                self._shuffle_buffer_size,
                str(self._seedseed) if self._seed is not None else "None"))
