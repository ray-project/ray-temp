from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import binascii
import hashlib
import numpy as np
import os
import sys
import uuid

import ray.local_scheduler

ERROR_KEY_PREFIX = b'Error:'
DRIVER_ID_LENGTH = 20


def _random_string():
    id_hash = hashlib.sha1()
    id_hash.update(uuid.uuid4().bytes)
    id_bytes = id_hash.digest()
    assert len(id_bytes) == 20
    return id_bytes


def format_error_message(exception_message, task_exception=False):
    """Improve the formatting of an exception thrown by a remote function.

    This method takes a traceback from an exception and makes it nicer by
    removing a few uninformative lines and adding some space to indent the
    remaining lines nicely.

    Args:
        exception_message (str): A message generated by traceback.format_exc().

    Returns:
        A string of the formatted exception message.
    """
    lines = exception_message.split('\n')
    if task_exception:
        # For errors that occur inside of tasks, remove lines 1 and 2 which are
        # always the same, they just contain information about the worker code.
        lines = lines[0:1] + lines[3:]
        pass
    return '\n'.join(lines)


def push_error_to_driver(redis_client,
                         error_type,
                         message,
                         driver_id=None,
                         data=None):
    """Push an error message to the driver to be printed in the background.

    Args:
        redis_client: The redis client to use.
        error_type (str): The type of the error.
        message (str): The message that will be printed in the background
            on the driver.
        driver_id: The ID of the driver to push the error message to. If this
            is None, then the message will be pushed to all drivers.
        data: This should be a dictionary mapping strings to strings. It
            will be serialized with json and stored in Redis.
    """
    if driver_id is None:
        driver_id = DRIVER_ID_LENGTH * b'\x00'
    error_key = ERROR_KEY_PREFIX + driver_id + b':' + _random_string()
    data = {} if data is None else data
    redis_client.hmset(error_key, {
        'type': error_type,
        'message': message,
        'data': data
    })
    redis_client.rpush('ErrorKeys', error_key)


def is_cython(obj):
    """Check if an object is a Cython function or method"""

    # TODO(suo): We could split these into two functions, one for Cython
    # functions and another for Cython methods.
    # TODO(suo): There doesn't appear to be a Cython function 'type' we can
    # check against via isinstance. Please correct me if I'm wrong.
    def check_cython(x):
        return type(x).__name__ == 'cython_function_or_method'

    # Check if function or method, respectively
    return check_cython(obj) or \
        (hasattr(obj, '__func__') and check_cython(obj.__func__))


def random_string():
    """Generate a random string to use as an ID.

    Note that users may seed numpy, which could cause this function to generate
    duplicate IDs. Therefore, we need to seed numpy ourselves, but we can't
    interfere with the state of the user's random number generator, so we
    extract the state of the random number generator and reset it after we are
    done.

    TODO(rkn): If we want to later guarantee that these are generated in a
    deterministic manner, then we will need to make some changes here.

    Returns:
        A random byte string of length 20.
    """
    # Get the state of the numpy random number generator.
    numpy_state = np.random.get_state()
    # Try to use true randomness.
    np.random.seed(None)
    # Generate the random ID.
    random_id = np.random.bytes(20)
    # Reset the state of the numpy random number generator.
    np.random.set_state(numpy_state)
    return random_id


def decode(byte_str):
    """Make this unicode in Python 3, otherwise leave it as bytes."""
    if sys.version_info >= (3, 0):
        return byte_str.decode('ascii')
    else:
        return byte_str


def binary_to_object_id(binary_object_id):
    return ray.ObjectID(binary_object_id)


def binary_to_hex(identifier):
    hex_identifier = binascii.hexlify(identifier)
    if sys.version_info >= (3, 0):
        hex_identifier = hex_identifier.decode()
    return hex_identifier


def hex_to_binary(hex_identifier):
    return binascii.unhexlify(hex_identifier)


def get_cuda_visible_devices():
    """Get the device IDs in the CUDA_VISIBLE_DEVICES environment variable.

    Returns:
        if CUDA_VISIBLE_DEVICES is set, this returns a list of integers with
            the IDs of the GPUs. If it is not set, this returns None.
    """
    gpu_ids_str = os.environ.get('CUDA_VISIBLE_DEVICES', None)

    if gpu_ids_str is None:
        return None

    if gpu_ids_str == '':
        return []

    return [int(i) for i in gpu_ids_str.split(',')]


def set_cuda_visible_devices(gpu_ids):
    """Set the CUDA_VISIBLE_DEVICES environment variable.

    Args:
        gpu_ids: This is a list of integers representing GPU IDs.
    """
    os.environ['CUDA_VISIBLE_DEVICES'] = ','.join([str(i) for i in gpu_ids])


def resources_from_resource_arguments(default_num_cpus, default_num_gpus,
                                      default_resources, runtime_num_cpus,
                                      runtime_num_gpus, runtime_resources):
    """Determine a task's resource requirements.

    Args:
        default_num_cpus: The default number of CPUs required by this function
            or actor method.
        default_num_gpus: The default number of GPUs required by this function
            or actor method.
        default_resources: The default custom resources required by this
            function or actor method.
        runtime_num_cpus: The number of CPUs requested when the task was
            invoked.
        runtime_num_gpus: The number of GPUs requested when the task was
            invoked.
        runtime_resources: The custom resources requested when the task was
            invoked.

    Returns:
        A dictionary of the resource requirements for the task.
    """
    if runtime_resources is not None:
        resources = runtime_resources.copy()
    elif default_resources is not None:
        resources = default_resources.copy()
    else:
        resources = {}

    if 'CPU' in resources or 'GPU' in resources:
        raise ValueError('The resources dictionary must not '
                         "contain the key 'CPU' or 'GPU'")

    assert default_num_cpus is not None
    resources['CPU'] = (default_num_cpus
                        if runtime_num_cpus is None else runtime_num_cpus)

    if runtime_num_gpus is not None:
        resources['GPU'] = runtime_num_gpus
    elif default_num_gpus is not None:
        resources['GPU'] = default_num_gpus

    return resources


def merge_dicts(d1, d2):
    """Merge two dicts and return a new dict that's their union."""
    d = d1.copy()
    d.update(d2)
    return d
