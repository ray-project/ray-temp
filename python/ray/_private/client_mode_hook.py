import os
from contextlib import contextmanager
from ray.experimental.client import ray

client_mode_enabled = os.environ.get("RAY_CLIENT_MODE", "0") == "1"

_client_hook_enabled = True

def _enable_client_hook(val: bool):
    global _client_hook_enabled
    _client_hook_enabled = val

def _disable_client_hook():
    global _client_hook_enabled
    out = _client_hook_enabled
    _client_hook_enabled = False
    return out

@contextmanager
def disable_client_hook():
    val = _disable_client_hook()
    try:
        yield None
    finally:
        _enable_client_hook(val)

def client_mode_hook(func):
    """
    Decorator for ray module methods to delegate to ray client
    """
    if not client_mode_enabled:
        return func

    def wrapper(*args, **kwargs):
        global _client_hook_enabled
        if _client_hook_enabled:
            return getattr(ray, func.__name__)(*args, **kwargs)
        return func(*args, **kwargs)
    wrapper.__client_hook_func = func
    return wrapper
