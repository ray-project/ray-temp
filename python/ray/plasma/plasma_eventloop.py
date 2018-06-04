import asyncio
import collections
import selectors

import ray


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


@asyncio.coroutine
def _wait(fs, timeout, num_returns, loop):
    """Enhancement of `asyncio.wait`.

    The fs argument must be a collection of Futures.
    """
    
    assert fs, 'Set of Futures is empty.'
    assert 0 < num_returns <= len(fs)
    
    waiter = loop.create_future()
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    
    n_finished = 0
    
    def _on_completion(f):
        nonlocal n_finished
        n_finished += 1
        
        if n_finished >= num_returns and (not f.cancelled() and f.exception() is not None):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)
    
    for f in fs:
        f.add_done_callback(_on_completion)
    
    try:
        yield from waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()
    
    done, pending = [], []
    for f in fs:
        f.remove_done_callback(_on_completion)
        if f.done():
            done.append(f)
        else:
            pending.append(f)
    return done, pending


class PlasmaObjectFuture(asyncio.Future):
    def __init__(self, loop, object_id):
        super().__init__(loop=loop)
        self.ref_count = 0
        self.object_id = object_id
    
    def inc_refcount(self):
        self.ref_count += 1
    
    def dec_refcount(self):
        assert self.ref_count > 0
        self.ref_count -= 1
        if self.ref_count <= 0:
            self.cancel()
    
    def complete(self):
        self.set_result(self.object_id)


class PlasmaPoll(selectors.BaseSelector):
    def __init__(self):
        self.waiting_dict = collections.defaultdict(list)
    
    def close(self):
        self.waiting_dict.clear()
    
    def select(self, timeout=None):
        ready_keys = []
        object_ids = ray.wait(list(self.waiting_dict.keys()), num_returns=len(self.waiting_dict), timeout=timeout)
        for oid in object_ids:
            key = self.waiting_dict[oid]
            ready_keys.append(key)
        return ready_keys
    
    def register(self, plasma_fut, events=None, data=None):
        if plasma_fut.object_id in self.waiting_dict:
            raise Exception('ObjectID already been registered.')
        else:
            key = selectors.SelectorKey(fileobj=plasma_fut, fd=plasma_fut.object_id, events=events, data=data)
            self.waiting_dict[key.fd] = key
            return key
    
    def unregister(self, plasma_fut):
        return self.waiting_dict.pop(plasma_fut.object_id)
    
    def get_map(self):
        return self.waiting_dict
    
    def get_key(self, object_id):
        return self.waiting_dict[object_id]


class PlasmaSelectorEventLoop(asyncio.BaseEventLoop):
    
    def __init__(self, selector=None):
        super().__init__()
        self._selector = PlasmaPoll() if selector is None else selector
    
    def _process_events(self, event_list):
        for key in event_list:
            handle, future = key.data
            assert isinstance(handle, asyncio.events.Handle), 'A Handle is required here'
            if handle._cancelled:
                return
            assert not isinstance(handle, asyncio.events.TimerHandle)
            self._ready.append(handle)
    
    def close(self):
        if self.is_running():
            raise RuntimeError("Cannot close a running event loop")
        if self.is_closed():
            return
        super().close()
        if self._selector is not None:
            self._selector.close()
            self._selector = None
    
    def _register_id(self, object_id):
        self._check_closed()
        
        try:
            key = self._selector.get_key(object_id)
        except KeyError:
            def callback(future):
                # set result and remove it from the selector
                if future.cancelled():
                    return
                future.complete()
                self._selector.unregister(future)
            
            fut = PlasmaObjectFuture(loop=self, object_id=object_id)
            handle = asyncio.events.Handle(callback, args=[fut], loop=self)
            self._selector.register(object_id, events=None, data=handle)
        else:
            # Keep a unique Future object for an object_id. Increase ref_count instead.
            fut = key.data
        
        fut.inc_refcount()
        
        return fut
    
    def _release(self, *fut):
        for f in fut:
            f.dec_refcount()
            if f.cancelled():
                self._selector.unregister(f)
    
    @asyncio.coroutine
    def get(self, object_ids):
        if not isinstance(object_ids, list):
            ready_id = yield from self._register_id(object_ids)
            return ray.get(ready_id)
        else:
            ready_ids = yield from asyncio.gather(*[self._register_id(oid) for oid in object_ids], loop=self)
            return ray.get(ready_ids)
    
    @asyncio.coroutine
    def wait(self, object_ids, num_returns=1, timeout=None):
        futures = [self._register_id(oid) for oid in object_ids]
        _done, _pending = yield from _wait(futures, timeout=timeout, num_returns=num_returns, loop=self)
        done = [fut.object_id for fut in _done]
        pending = [fut.object_id for fut in _pending]
        self._release(*pending)
        return done, pending
