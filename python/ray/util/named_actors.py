import ray
import ray.cloudpickle as pickle
from ray.experimental.internal_kv import _internal_kv_get, _internal_kv_put
from ray._raylet import gcs_actor_service_enabled


def _calculate_key(name):
    """Generate a Redis key with the given name.

    Args:
        name: The name of the named actor.

    Returns:
        The key to use for storing a named actor in Redis.
    """
    return b"Actor:" + name.encode("ascii")


def get_actor(name):
    """Get a named actor which was previously created.

    If the actor doesn't exist, an exception will be raised.

    Args:
        name: The name of the named actor.

    Returns:
        The ActorHandle object corresponding to the name.
    """
    if gcs_actor_service_enabled():
        worker = ray.worker.global_worker
        handle = worker.core_worker.get_named_actor_handle(name)
    else:
        actor_name = _calculate_key(name)
        pickled_state = _internal_kv_get(actor_name)
        if pickled_state is None:
            raise KeyError("The actor with name={} doesn't exist".format(name))
        handle = pickle.loads(pickled_state)

    return handle


def register_actor(name, actor_handle):
    """Register a named actor under a string key.

   Args:
       name: The name of the named actor.
       actor_handle: The actor object to be associated with this name
   """
    if not isinstance(name, str):
        raise TypeError("The name argument must be a string.")
    if not isinstance(actor_handle, ray.actor.ActorHandle):
        raise TypeError("The actor_handle argument must be an ActorHandle "
                        "object.")
    actor_name = _calculate_key(name)

    # First check if the actor already exists.
    try:
        get_actor(name)
        exists = True
    except ValueError:
        exists = False

    if exists:
        raise ValueError("An actor with name={} already exists".format(name))

    # Add the actor to Redis if it does not already exist.
    _internal_kv_put(actor_name, pickle.dumps(actor_handle))
