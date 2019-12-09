"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import ray
import asyncio
from collections import namedtuple


def sync_to_async(func):
    """Convert a blocking function to async function"""

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# Class encapsulate the get result from direct actor.
# Case 1: plasma_fallback_id=None, result=<Object>
# Case 2: plasma_fallback_id=ObjectID, result=None
AsyncGetResponse = namedtuple("AsyncGetResponse",
                              ["plasma_fallback_id", "result"])


def get_async(object_id):
    """Asyncio compatible version of ray.get"""
    # Delayed import because raylet import this file and
    # it creates circular imports.
    from ray.experimental.async_api import init as async_api_init, as_future
    from ray.experimental.async_plasma import PlasmaObjectFuture

    assert isinstance(object_id, ray.ObjectID), "Batched get is not supported."

    # Setup
    async_api_init()
    loop = asyncio.get_event_loop()
    core_worker = ray.worker.global_worker.core_worker

    # Here's the callback used to implement async get logic.
    # What we want:
    # - If direct call, first try to get it from in memory store.
    #   If the object if promoted to plasma, retry it from plasma API.
    # - If not direct call, directly use plasma API to get it.
    user_future = loop.create_future()

    # We have three future objects here.
    # user_future is directly returned to the user from this function.
    #     and it will be eventually fulfilled by the final result.
    # inner_future is the first attempt to retrieve the object. It can be
    #     fulfilled by either core_worker.get_async or plasma_api.as_future.
    #     When inner_future completes, done_callback will be invoked. This
    #     callback set the final object in user_future if the object hasn't
    #     been promoted by plasma, otherwise it will retry from plasma.
    # retry_lasma_future is only created when we are getting objects that's
    #     promoted to plasma. It will also invoke the done_callback when it's
    #     fulfilled.

    def done_callback(future):
        result = future.result()
        # Result from async plasma, transparently pass it to user future
        if isinstance(future, PlasmaObjectFuture):
            user_future.set_result(result)
        else:
            # Result from direct call.
            assert isinstance(result, AsyncGetResponse), result
            if result.plasma_fallback_id is None:
                user_future.set_result(result.result)
            else:
                # Schedule plasma to async get, use the the same callback.
                retry_plasma_future = as_future(result.plasma_fallback_id)
                retry_plasma_future.add_done_callback(done_callback)
                # A hack to keep reference to the future so it doesn't get GC.
                user_future.retry_plasma_future = retry_plasma_future

    if object_id.is_direct_call_type():
        inner_future = loop.create_future()
        core_worker.in_memory_store_get_async(object_id, inner_future)
    else:
        inner_future = as_future(object_id)
    inner_future.add_done_callback(done_callback)
    # A hack to keep reference to inner_future so it doesn't get GC.
    user_future.inner_future = inner_future

    return user_future
