import random
from typing import Callable, List, Union, Iterable, Iterator

import pandas as pd
from pandas import DataFrame

from ray.util.iter import (_ActorSet, _NextValueNotReady, LocalIterator,
                           ParallelIterator, T, U)


class MLDataset(ParallelIterator[DataFrame]):
    """ A distributed ML dataset implemented based on ParallelIterator

    All item should be a list like object or dataclass instance.

    Args:
        batch_size (int): The batch size of the current dataset. It should be
            larger than zero, and 0 means unknown.
    """

    def __init__(self,
                 actor_sets: List["_ActorSet"],
                 name: str,
                 parent_iterators: List[ParallelIterator[DataFrame]],
                 batch_size: int,
                 repeated: bool):
        super(MLDataset, self).__init__(actor_sets, name, parent_iterators)
        self._batch_size = batch_size
        self._repeated = repeated

    @staticmethod
    def from_parallel_it(para_it: ParallelIterator[DataFrame],
                         batch_size: int,
                         repeated: bool) -> "MLDataset":
        """
        Create a MLDataset from an existing parallel iterator and each
        object is a pandas.DataFrame
        Args:
            para_it (ParallelIterator[T]): An existing parallel iterator, and each
                should be a list like object or dataclass instance.
            batch_size (int): The batch size of the current dataset. It should be
                larger than zero, and 0 means unknown.
            repeated (bool): whether the para_it is repeated.
        Returns:
            A MLDataset
        """
        return MLDataset(para_it.actor_sets, para_it.name,
                         para_it.parent_iterators, batch_size, repeated)

    def __iter__(self):
        raise TypeError(
            "You must use it.gather_sync() or it.gather_async() to "
            "iterate over the results of a MLDataset.")

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return f"MLDataset[{self.name}]"

    def _with_transform(self, local_it_fn, name) -> "MLDataset":
        """Helper function to create new MLDataset"""
        para_it = super()._with_transform(local_it_fn, name)
        return MLDataset.from_parallel_it(
            para_it, self._batch_size, self._repeated)

    def transform(self, fn: Callable[[Iterable[DataFrame]], Iterable[DataFrame]]
                  ) -> "MLDataset":
        """
        Apply the fn function to the MLDataset
        Args:
            fn (Callable[[Iterable[DataFrame]], Iterable[DataFrame]]):
                The function to applied. The input is a iterator of
                pandas.DataFrame, and the output should also be a iterator of
                pandas.DataFrame.
        Returns:
            A new MLDataset
        """
        return self._with_transform(lambda local_it: local_it.transform(fn),
                                    ".transform()")

    def batch(self, batch_size: int) -> "MLDataset":
        """
        Unlike the ParallelIterator.batch. This method rebatch the underlying
        the pandas DataFrame, and each pandas DataFrame will have batch_size
        rows.
        """
        if batch_size == self._batch_size:
            return self

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

        self._batch_size = batch_size
        return self._with_transform(
            lambda local_it: local_it.transform(batch_fn),
            f".batch({batch_size})")

    def flatten(self) -> "MLDataset":
        raise Exception("Unsupported operation")

    def combine(self, fn: Callable[[T], List[U]]) -> "MLDataset":
        raise Exception("Unsupported operation")

    def repeated(self) -> bool:
        return self._repeated

    def local_shuffle(self, shuffle_buffer_size: int,
                      seed: int = None) -> "MLDataset":
        """
        Unlike the ParallelIterator.local_shuffle. This shuffle will first
        apply the local_shuffle for each shards and then shuffle the each
        pandas DataFrame.
        """
        ds = super().local_shuffle(shuffle_buffer_size, seed)

        def shuffle_fn(it: Iterable[DataFrame]) -> Iterable[DataFrame]:
            for df in it:
                df = df.sample(frac=1, random_state=seed)
                yield df

        ds = ds._with_transform(
            lambda local_it: local_it.transform(shuffle_fn),
            ".inner_pandas_shuffle()")

        return ds

    def repartition(self, num_partitions: int,
                    batch_ms: int = 0) -> "MLDataset":
        """see ParallelIterator.repartition"""
        if num_partitions == self.num_shards():
            return self
        para_it = super().repartition(num_partitions, batch_ms)
        return MLDataset.from_parallel_it(para_it, self._batch_size)

    def union(self, other: "MLDataset") -> "MLDataset":
        """Return an iterator that is the union of this and the other."""
        if not isinstance(other, MLDataset):
            raise TypeError(
                f"other must be of type MLDataset, got {type(other)}")

        if self._repeated != other.repeated():
            raise TypeError(
                f"want to union two MLDataset which have different repeated "
                f"type, self repeated: {self._repeated}, other repeated: "
                f"{other.repeated()}"
            )

        batch_size = 0
        if self._batch_size == other._batch_size:
            batch_size = self._batch_size

        actor_sets = []
        actor_sets.extend(self.actor_sets)
        actor_sets.extend(other.actor_sets)
        # if one of these iterators is a result of a repartition, we need to
        # keep an explicit reference to its parent iterator
        return MLDataset(
            actor_sets,
            f"ParallelUnion[{self}, {other}]",
            parent_iterators=self.parent_iterators + other.parent_iterators,
            batch_size=batch_size,
            repeated=self._repeated)

    def select_shards(self,
                      shards_to_keep: List[int]) -> "MLDataset":
        para_it = super().select_shards(shards_to_keep)
        return MLDataset.from_parallel_it(
            para_it, self._batch_size, self._repeated)

    def get_repeat_shard(self,
                         index: int,
                         batch_ms: int = 0,
                         num_async: int = 1,
                         shuffle: bool = False,
                         shuffle_buffer_size: int = 1,
                         seed: int = None) -> Iterator[DataFrame]:
        """
        Get the given shard of the current dataset. The return is a iterator.
        We support shuffle the return iterator when each call iter on the
        return.
        Args:
            index (int): the shard index id
            batch_ms (int): Batches items for batch_ms milliseconds
                before retrieving it.
                Increasing batch_ms increases latency but improves throughput.
                If this value is 0, then items are returned immediately.
            num_async (int): The max number of requests in flight.
                Increasing this improves the amount of pipeline
                parallelism in the iterator.
            shuffle (bool): whether shuffle the given shard data
            shuffle_buffer_size (int): same as ParallelIterator.local_shuffle
            seed (int): the random seed
        Returns:
            The given shard iterator. If the shuffle is True, each call iter
            will return a different ordered iterator.
        """
        return _RepeatableIterator(self, index, batch_ms, num_async,
                                   shuffle, shuffle_buffer_size, seed)

    def to_torch(self,
                 feature_columns=None,
                 feature_shapes=None,
                 feature_types=None,
                 label_column=None,
                 label_shape=None,
                 label_type=None):
        """
        Create a TorchDataset from the current DistributedDataset.
        Args:
            feature_columns (List[Union[int, str]]): the column indexes
                name. This is a list of int if the record is list like object.
                This is a list of str if the record is dataclass instance.
            feature_shapes (Optional[List[Any]]): the feature shapes matching
               the feature columns. One row will packet into one torch.Tensor
               if this is not provided. Otherwise, each feature column will be
               one torch.Tensor and with the provided shapes.
            feature_types (Optional[List["torch.dtype"]]): the feature types
               matching the feature columns. All feature will be cast into
               torch.float by default. Otherwise, cast into the provided type.
            label_column (Union[int, str]): the label index or name. This is a
               int index if the record is list like object. It should be str if
               the record is dataclass instance.
            label_shape (Optional[int]): the label shape.
            label_type (Optional["torch.dtype"]): the label type, this will be
               cast into torch.float by default
        Returns:
            A TorchDataset
        """
        from ray.util.sgd.torch.torch_dataset import TorchDataset
        return TorchDataset(self, feature_columns, feature_shapes,
                            feature_types, label_column, label_shape,
                            label_type)

    def to_tf(self,
              feature_columns=None,
              feature_shapes=None,
              feature_types=None,
              label_column=None,
              label_shape=None,
              label_type=None):
        """
        Create a TFDataset from the current DistributedDataset. This will
        convert to a PandasDistributedDataset first, and then convert a
        TFDataset.
        Args:
            feature_columns (List[Union[int, str]]): the column indexes
                name. This is a list of int if the record is list like object.
                This is a list of str if the record is dataclass instance.
            feature_shapes (Optional[List[tf.TensorShape]]): the feature shapes
                matching the feature columns. One row will packet into one
                tf.Tensor if this is not provided. Otherwise, each feature
                column will be one tf.Tensor and with the provided shapes.
            feature_types (Optional[List["tf.DType"]]): the feature types
               matching the feature columns. All feature will be cast into
               tf.float by default. Otherwise, cast into the provided type.
            label_column (Union[int, str]): the label index or name. This is a
               int index if the record is list like object. It should be str if
               the record is dataclass instance.
            label_shape (Optional[tf.TensorShape]): the label shape.
            label_type (Optional["tf.DType"]): the label type, this will be
               cast into tf.float by default
        Returns:
            A TFDataset
        """
        from ray.util.sgd.tf.tf_dataset import TFDataset
        return TFDataset(self, feature_columns, feature_shapes, feature_types,
                         label_column, label_shape, label_type)


class _RepeatableIterator(Iterator[T]):
    """
    A repeatable iterator for the given shard index data. Each call
    iter(_RepeatableIterator instance) will shuffle the iterator and return a
    different order or data.
    Args:
        ds (MLDataset): a MLDataset
        shard_index (int): the shard index id.
        batch_ms (int): Batches items for batch_ms milliseconds
                before retrieving it.
                Increasing batch_ms increases latency but improves throughput.
                If this value is 0, then items are returned immediately.
        num_async (int): The max number of requests in flight.
            Increasing this improves the amount of pipeline
            parallelism in the iterator.
        shuffle (bool): whether shuffle the given shard data
        shuffle_buffer_size (int): same as ParallelIterator.local_shuffle
        seed (int): the random seed
    """

    def __init__(self,
                 ds: MLDataset,
                 shard_index: int,
                 batch_ms: int = 0,
                 num_async: int = 1,
                 shuffle: bool = False,
                 shuffle_buffer_size: int = 1,
                 seed: int = None):
        super(_RepeatableIterator, self).__init__()
        self._ds = ds
        self._shard_index = shard_index
        self._batch_ms = batch_ms
        self._num_async = num_async
        self._shuffle = shuffle
        self._shuffle_buffer_size = shuffle_buffer_size
        self._seed = seed
        self._local_it: LocalIterator[T] = None

    def __next__(self) -> T:
        assert self._local_it is not None
        return next(self._local_it)

    def __iter__(self) -> Iterator[T]:
        it = self._ds.get_shard(self._shard_index, self._batch_ms,
                                self._num_async)
        if self._shuffle:
            it = self.shuffle(it)

        self._local_it = it
        return self

    def shuffle(self, local_it: LocalIterator[T]) -> LocalIterator[DataFrame]:
        shuffle_random = random.Random(self._seed)

        def apply_shuffle(it):
            buffer = []
            for item in it:
                if isinstance(item, _NextValueNotReady):
                    yield item
                else:
                    buffer.append(item)
                    if len(buffer) >= self._shuffle_buffer_size:
                        item = buffer.pop(
                            shuffle_random.randint(0,
                                                   len(buffer) - 1))
                        item = item.sample(frac=1, random_state=self._seed)
                        yield item
            while len(buffer) > 0:
                item = buffer.pop(shuffle_random.randint(0, len(buffer) - 1))
                item = item.sample(frac=1, random_state=self._seed)
                yield item

        return LocalIterator(
            local_it.base_iterator,
            local_it.shared_metrics,
            local_it.local_transforms + [apply_shuffle],
            name=local_it.name +
            ".shuffle(shuffle_buffer_size={}, seed={})".format(
                self._shuffle_buffer_size,
                str(self._seed) if self._seed is not None else "None"))
