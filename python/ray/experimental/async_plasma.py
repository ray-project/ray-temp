import asyncio
import functools
import inspect
import sys
from typing import Union, List, Callable, Dict, Awaitable

import pyarrow.plasma as plasma

import ray
from ray.services import logger

RayAsyncType = Union[ray.ObjectID, Awaitable]
RayAsyncParamsType = Union[RayAsyncType, List[RayAsyncType]]


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


class PlasmaProtocol(asyncio.Protocol):

    def __init__(self, loop, plasma_client,
                 plasma_event_handler: 'PlasmaEventHandler'):
        self.plasma_client = plasma_client
        self.plasma_event_handler = plasma_event_handler
        self.transport = None
        self.on_con_lost = loop.create_future()
        self._buffer = b''

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self._buffer += data
        messages = []
        i = 0
        while i + 8 <= len(self._buffer):
            msg_len = int.from_bytes(self._buffer[i:i + 8], sys.byteorder)
            if i + 8 + msg_len > len(self._buffer):
                break
            i += 8
            segment = self._buffer[i: i + msg_len]
            i += msg_len
            messages.append(self.plasma_client.decode_notification(segment))

        self._buffer = self._buffer[i:]
        self.plasma_event_handler.process_notifications(messages)

        # We are done: close the transport;
        # connection_lost() will be called automatically.
        # self.transport.close()

    def connection_lost(self, exc):
        # The socket has been closed
        self.on_con_lost.set_result(True)

    def eof_received(self):
        logger.debug('EOF received')
        self.transport.close()


class PlasmaObjectFuture(asyncio.Future):
    """This class manages the lifecycle of a Future contains an object_id.

    Note:
        This Future should be removed from a listening selector
        when the object_id is done or the ref_count goes to zero.

        Each time we add a task depending on this Future,
        we increase its refcount. When the task is cancelled or timeout,
        the refcount should be decreased.

    Attributes:
        ref_count(int): The reference count.
        object_id: The object_id this Future contains.

    """

    def __init__(self, loop, object_id):
        super().__init__(loop=loop)
        self.ref_count = 0
        self.object_id = object_id
        self.prev = None
        self.next = None

    @property
    def ray_object_id(self):
        return ray.ObjectID(self.object_id.binary())

    def complete(self):
        self.set_result(self.object_id)

    def __repr__(self):
        return super().__repr__() + "{object_id=%s, ref_count=%d}" % (
            self.object_id, self.ref_count)


class PlasmaObjectLinkedList(asyncio.Future):
    def __init__(self, loop, object_id: ray.ObjectID):
        super().__init__(loop=loop)
        self.object_id = object_id
        self.head: PlasmaObjectFuture = None
        self.tail: PlasmaObjectFuture = None

    def append(self, future: PlasmaObjectFuture):
        future.prev = self.tail
        if self.tail is None:
            assert self.head is None
            self.head = future
        else:
            self.tail.next = future
        self.tail = future
        # Once done, it will be removed from the list.
        future.add_done_callback(self.remove)

    def remove(self, future: PlasmaObjectFuture):
        if self._loop.get_debug():
            logger.info("Removing %s from the linked list.", future)
        if future.prev is None:
            assert future is self.head
            self.head = future.next
            if self.head is None:
                self.tail = None
                self.set_result(None)
            else:
                self.head.prev = None
        elif future.next is None:
            assert future is self.tail
            self.tail = future.prev
            if self.tail is None:
                self.head = None
                self.set_result(None)
            else:
                self.tail.prev = None

    def traverse(self):
        current = self.head
        while current is not None:
            yield current
            current = current.next


class PlasmaFutureGroup(asyncio.Future):
    """This class groups futures for better management and advanced operation.
    """

    def __init__(self, loop, return_exceptions=False, keep_duplicated=True):
        """Initialize this class.

        Args:
            loop (PlasmaSelectorEventLoop): An eventloop.
            return_exceptions(bool): If true, return exceptions as results
                instead of raising them.
            keep_duplicated(bool): If true,
                an future can be added multiple times.
        """

        super().__init__(loop=loop)
        self._children = []
        self._future_set = set()
        self._keep_duplicated = keep_duplicated
        self.return_exceptions = return_exceptions
        self.nfinished = 0

    def append(self, coroutine_or_future):
        """This method append a coroutine or a future into the group

        Args:
            coroutine_or_future: A coroutine or a future object.
        """

        if not asyncio.futures.isfuture(coroutine_or_future):
            fut = asyncio.ensure_future(coroutine_or_future, loop=self._loop)
            if self._loop is None:
                self.loop = fut._loop
            # The caller cannot control this future, the "destroy pending task"
            # warning should not be emitted.
            fut._log_destroy_pending = False
        else:
            fut = coroutine_or_future

            if self._loop is None:
                self._loop = fut._loop
            elif fut._loop is not self._loop:
                raise ValueError("futures are tied to different event loops")

        if fut in self._future_set and not self._keep_duplicated:
            return
        fut.add_done_callback(self._done_callback)
        self._children.append(fut)
        self._future_set.add(fut)

    def extend(self, iterable):
        """This method behaves like `list.extend`"""

        for item in iterable:
            self.append(item)

    def pop(self, index=0):
        fut = self._children.pop(index)
        fut.remove_done_callback(self._done_callback)
        self._future_set.remove(fut)
        return fut

    def clear(self):
        while self._children:
            self.pop()

    @property
    def children(self):
        return self._children

    @property
    def nchildren(self):
        return len(self._children)

    def halt_on_all_finished(self):
        return self.nfinished >= len(self._children)

    def halt_on_any_finished(self):
        return self.nfinished > 0

    def halt_on_some_finished(self, n):
        return self.nfinished >= n

    def _halt_on(self):
        """This function can be override to change the halt condition.

        Returns:
            bool: True if we meet the halt condition.
        """

        return self.halt_on_all_finished()

    def set_halt_condition(self, cond: Callable[['PlasmaFutureGroup'], bool]):
        """This function sets the halting condition.

        Args:
            cond (Callable): Halting condition.
        """

        self._halt_on = cond

    def _collect_results(self):
        results = []

        for fut in self._children:
            if fut.cancelled() and self.return_exceptions:
                res = asyncio.futures.CancelledError()
            elif fut._exception is not None and self.return_exceptions:
                res = fut.exception()  # Mark exception retrieved.
            else:
                res = fut._result
            results.append(res)

        return results

    def _done_callback(self, fut: asyncio.Future):
        if self.done():
            if not fut.cancelled():
                # Mark exception retrieved.
                fut.exception()
            return

        if not self.return_exceptions:
            if fut.cancelled():
                self.set_exception(asyncio.futures.CancelledError())
                return
            elif fut._exception is not None:
                self.set_exception(fut.exception())
                return

        self.nfinished += 1

        if self._halt_on():
            self.set_result(self._collect_results())

    def __iter__(self):
        # Deal with the case that there's nothing to group.
        if not self.children:
            self.set_result([])
        return super().__iter__()

    def __await__(self):
        # Deal with the case that there's nothing to group.
        if not self.children:
            self.set_result([])
        return super().__await__()

    def cancel(self):
        if self.done():
            return False
        ret = False
        for child in self._children:
            if child.cancel():
                ret = True
        return ret

    def flush_results(self):
        done, pending = [], []
        for f in self._children:
            if f.done():
                done.append(f)
            else:
                pending.append(f)
        return done, pending

    async def wait(self, timeout: float = None):
        if not self._children:
            return [], []

        assert timeout is None or timeout >= 0
        if timeout == 0:
            return self.flush_results()

        loop = self._loop

        waiter = loop.create_future()
        timeout_handle = None
        if timeout is not None:
            timeout_handle = loop.call_later(timeout, _release_waiter, waiter)

        def _on_completion(_):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

        self.add_done_callback(_on_completion)

        try:
            await waiter
        finally:
            if timeout_handle is not None:
                timeout_handle.cancel()

        self.remove_done_callback(_on_completion)

        return self.flush_results()


class PlasmaEventHandler:
    """This class is an event handler for Plasma."""

    def __init__(self, loop, worker):
        super().__init__()
        self._loop: asyncio.BaseEventLoop = loop
        self._worker = worker
        self._waiting_dict: Dict[ray.ObjectID, PlasmaObjectLinkedList] = {}

    def process_notifications(self, messages: List):
        for object_id, object_size, metadata_size in messages:
            if object_id in self._waiting_dict:
                linked_list = self._waiting_dict[object_id]
                for future in linked_list.traverse():
                    if future.cancelled():
                        continue
                    future.complete()

    def close(self):
        self._waiting_dict.clear()

    def _unregister_callback(self, fut: PlasmaObjectLinkedList):
        del self._waiting_dict[fut.object_id]

    def as_future(self, object_id: ray.ObjectID):
        """Turn an object_id into a Future object.

        Args:
            object_id: A Ray's object_id.

        Returns:
            PlasmaObjectFuture: A future object that waits the object_id/
        """
        if not isinstance(object_id, ray.ObjectID):
            raise TypeError("Input should be an ObjectID.")

        ready, _ = ray.wait([object_id], timeout=0)
        plain_object_id = plasma.ObjectID(object_id.id())
        fut = PlasmaObjectFuture(loop=self._loop, object_id=plain_object_id)

        if ready:
            if self._loop.get_debug():
                logger.info("%s has been ready.", plain_object_id)
            fut.complete()
            return fut

        if plain_object_id not in self._waiting_dict:
            linked_list = PlasmaObjectLinkedList(self._loop, plain_object_id)
            linked_list.add_done_callback(self._unregister_callback)
            self._waiting_dict[plain_object_id] = linked_list
        self._waiting_dict[plain_object_id].append(fut)
        if self._loop.get_debug():
            logger.info("%s added to the waiting list.", fut)

        return fut

    def _convert(self, object_ids):
        processed = []
        selection_vector = []
        for i, obj in enumerate(object_ids):
            if isinstance(obj, ray.ObjectID):
                processed.append(self.as_future(obj))
                selection_vector.append(i)
            elif inspect.isawaitable(obj):
                processed.append(obj)
            else:
                raise TypeError("Unsupported type %s" % type(obj))
        return processed, selection_vector

    async def get(self, object_ids: RayAsyncParamsType):
        if not isinstance(object_ids, list):
            if inspect.isawaitable(object_ids):
                return await object_ids
            plain_ready_id = await self.as_future(object_ids)
            return self._worker.retrieve_and_deserialize([plain_ready_id], 0)[
                0]
        else:
            processed, selection_vector = self._convert(object_ids)
            fut = PlasmaFutureGroup(loop=self._loop, return_exceptions=False)
            fut.extend(processed)
            results = await fut
            plain_ready_ids = [results[i] for i in selection_vector]
            objs = self._worker.retrieve_and_deserialize(plain_ready_ids, 0)
            for i, obj in enumerate(objs):
                results[selection_vector[i]] = obj
            return results

    async def wait(self,
                   object_ids: List[Union[ray.ObjectID, Awaitable]],
                   num_returns=1,
                   timeout=None,
                   return_exact_num=True):
        """This method corresponds to `ray.wait`.

        Args:
            object_ids (RayAsyncParamsType):
                A list of single object_id, future, coroutine.
            timeout (float):
                The timeout in seconds (`ray.wait` use milliseconds).
            num_returns (int): The minimal number of ready object returns.
            return_exact_num (bool): If true, return no more than the amount
                specified.

        Returns:
            Tuple[List, List]: Ready futures & unready ones.
        """

        processed, selection_vector = self._convert(object_ids)
        temp = {processed[i] for i in selection_vector}

        fut = PlasmaFutureGroup(loop=self._loop, return_exceptions=False)
        fut.set_halt_condition(
            functools.partial(
                fut.halt_on_some_finished,
                n=num_returns,
            ))
        fut.extend(processed)
        _done, _pending = await fut.wait(timeout)
        fut.return_exceptions = True  # Ignore `CancelledError`.

        # Release temporary futures
        for f in _pending:
            if isinstance(f, PlasmaObjectFuture) and f in temp:
                f.cancel()

        done = [fut.ray_object_id if isinstance(fut, PlasmaObjectFuture) else
                fut for fut in _done]
        pending = [fut.ray_object_id if isinstance(fut, PlasmaObjectFuture)
                   else fut for fut in _pending]

        if return_exact_num and len(done) > num_returns:
            done, pending = done[:num_returns], done[num_returns:] + pending
        return done, pending
