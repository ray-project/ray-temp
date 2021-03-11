from ray.serve.api import (accept_batch, Client, connect, start,
                           get_replica_context)
from ray.serve.config import BackendConfig, HTTPOptions

# Mute the warning because Serve sometimes intentionally calls
# ray.get inside async actors.
import ray.worker
ray.worker.blocking_get_inside_async_warned = True

__all__ = [
    "accept_batch", "BackendConfig", "connect", "Client", "start",
    "HTTPOptions", "get_replica_context"
]
