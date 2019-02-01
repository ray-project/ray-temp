from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import time

import ray

START_SIGNAL_COUNTER = 10000

class Signal(object):
    """Signal object"""
    pass

class DoneSignal(Signal):
    pass

class ErrorSignal(Signal):
    def __init__(self, error):
        self.error = error

def _get_task_id(source_id):
    if  type(source_id) is ray.actor.ActorHandle:
        return ray._raylet.compute_task_id(source_id._ray_actor_creation_dummy_object_id)
    else:
        # TODO(pcm): Figure out why compute_signal_id is called with both
        # object and task ids.
        if type(source_id) is ray.TaskID:
            return source_id
        else:
            return ray._raylet.compute_task_id(source_id)

def _get_signal_id(source_id, counter):
    return ray._raylet.compute_signal_id(_get_task_id(source_id), counter)

def task_id(object_id):
    return ray._raylet.compute_task_id(object_id)

def send(signal, source_id = None):
    """Send signal on behalf of source_id.
    Each signal is identified by (source_id, index), where index is incremented
    every time a signal is sent, starting from 1. Receiving this signal,
    requires waiting on (source_id, index).
    Args:
        signal: signal to be sent.
        source_id: If empty, initialize to the id of the task/actor
                   invoking this function.
    """
    if source_id == None:
        if hasattr(ray.worker.global_worker, "actor_creation_task_id"):
            source_key = ray.worker.global_worker.actor_creation_task_id.binary()
        else:
            # no actors; this function must have been called from a task
            source_key = ray.worker.global_worker.current_task_id.binary()
    else:
        source_key = source_id.binary()

    index = ray.worker.global_worker.redis_client.incr(source_key)
    if index < START_SIGNAL_COUNTER:
        ray.worker.global_worker.redis_client.set(source_key, START_SIGNAL_COUNTER)
        index = START_SIGNAL_COUNTER

    object_id = _get_signal_id(ray.ObjectID(source_key), index)
    ray.worker.global_worker.store_and_register(object_id, signal)

def receive(source_ids, timeout=float('inf')):
    """Get all signals from each source in source_ids.
    For each source_id in source_ids, this function returns all signals
    generated by (or on behalf of) source_id since the last receive() or
    forget() were invoked on source_id. If this is the first call on
    source_id, this function returns all signals generated by (or on
    behalf of) source_id so far.
    Args:
        source_ids: list of source ids whose signals are returned.
        timeout: time it receives for new signals to be generated. If none,
                 return when timeout expires. Measured in seconds.
    Returns:
        The list of signals generated for each source in source_ids. They
        are returned as a list of pairs (source_id, signal). There can be
        more than a signal for the same source_id.
    """
    if not hasattr(ray.worker.global_worker, "signal_counters"):
        ray.worker.global_worker.signal_counters = dict()

    signal_counters = ray.worker.global_worker.signal_counters
    results = []
    previous_time = time.time()
    remaining_time = timeout

    # If we never received a signal from a source_id, initialize the
    # signal couunter for source_id to START_SIGNAL_COUNTER.
    for source_id in source_ids:
        if not source_id in signal_counters:
            signal_counters[source_id] = START_SIGNAL_COUNTER

    # Store the reverse mapping from signals to
    # source ids in the source_id_from_signal_id dictionary.
    source_id_from_signal_id = dict()
    for source_id in source_ids:
        signal_id = _get_signal_id(source_id, signal_counters[source_id])
        source_id_from_signal_id[signal_id] = source_id

    while True:
        ready_ids, _ = ray.wait(source_id_from_signal_id.keys(),
            num_returns=len(source_id_from_signal_id.keys()), timeout=0)
        if len(ready_ids) > 0:
            for signal_id in ready_ids:
                signal = ray.get(signal_id)
                source_id = source_id_from_signal_id[signal_id]
                if isinstance(signal, Signal):
                    results.append((source_id, signal))
                    if type(signal) == DoneSignal:
                        del signal_counters[source_id]

                # We read this signal so forget it.
                del source_id_from_signal_id[signal_id]

                if source_id in signal_counters:
                    # Compute id of the next expected signal for this source id.
                    signal_counters[source_id] += 1
                    signal_id = _get_signal_id(source_id, signal_counters[source_id])
                    source_id_from_signal_id[signal_id] = source_id
                else:
                    break
            current_time = time.time()
            remaining_time -= (current_time - previous_time)
            previous_time = current_time
            if remaining_time < 0:
                break
        else:
            break


    if (remaining_time < 0) or (len(results) > 0):
        return results

    # Thee are no past signals, and the timeout has not expired yet.
    # Wait for future signals or until timeout expires.
    ready_ids, _ = ray.wait(source_id_from_signal_id.keys(), 1, timeout=remaining_time)

    for ready_id in ready_ids:
        signal_counters[source_id_from_signal_id[ready_id]] += 1
        signal = ray.get(signal_id)
        if isinstance(signal, Signal):
            results.append((source_id, signal))
            if type(signal) == DoneSignal:
                del signal_counters[source_id]

    return results


def forget(source_ids):
    """Ignore all previous signals of each source_id in source_ids.
    The index of the next expected signal from source_id is set to the
    last signal's index plus 1. This means that the next receive() on source_id
    will only get the signals generated by (or on behalf to) source_id after
    this function was invoked.
    Args:
        source_ids: list of source ids whose past signals are forgotten.
    """
    if not hasattr(ray.worker.global_worker, "signal_counters"):
        ray.worker.global_worker.signal_counters = dict()
    signal_counters = ray.worker.global_worker.signal_counters

    for source_id in source_ids:
        source_key = ray._raylet.compute_task_id(source_id._ray_actor_creation_dummy_object_id).binary()
        value = ray.worker.global_worker.redis_client.get(source_key)
        if value != None:
            signal_counters[source_id] = int(value) + 1
        else:
            signal_counters[source_id] = START_SIGNAL_COUNTER

def reset():
    """
    Reset the worker state associated with any signals that this worker
    has received so far.
    If the worker calls receive() on a source_id next, it will get all the
    signals generated by (or on behalf of) source_id from the beginning.
    """
    ray.worker.global_worker.signal_counters = dict()
