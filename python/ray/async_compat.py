"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import asyncio
from collections import namedtuple
import time
import inspect

try:
    import uvloop
except ImportError:
    uvloop = None

import ray


def get_new_event_loop():
    """Construct a new event loop. Ray will use uvloop if it exists"""
    if uvloop:
        return uvloop.new_event_loop()
    else:
        return asyncio.new_event_loop()


def sync_to_async(func):
    """Convert a blocking function to async function"""

    if inspect.iscoroutinefunction(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# Class encapsulate the get result from direct actor.
# Case 1: plasma_fallback_id=None, result=<Object>
# Case 2: plasma_fallback_id=ObjectID, result=None
AsyncGetResponse = namedtuple("AsyncGetResponse",
                              ["plasma_fallback_id", "result"])


def get_async(object_id):
    """C++ Asyncio version of ray.get"""
    loop = asyncio.get_event_loop()
    core_worker = ray.worker.global_worker.core_worker

    future = loop.create_future()
    core_worker.get_async(object_id, future)
    # A hack to keep a reference to the object ID for ref counting.
    future.object_id = object_id
    return future
