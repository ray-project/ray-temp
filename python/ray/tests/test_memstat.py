import ray
import numpy as np
import time
from ray.internal.internal_api import memstat
from ray.test_utils import SignalActor

# Unique strings.
DRIVER_PID = "driver pid"
WORKER_PID = "worker pid"
UNKNOWN_SIZE = " ? "

# Reference status.
LOCAL_REF = "LOCAL_REFERENCE"
USED_BY_PENDING_TASK = "USED_BY_PENDING_TASK"
CAPTURED_IN_OBJECT = "CAPTURED_IN_OBJECT"

# Call sites.
PUT_OBJ = "(put object)"
TASK_CALL_OBJ = "(task call)"
ACTOR_TASK_CALL_OBJ = "(actor call)"
DESER_TASK_ARG = "(deserialize task arg)"
DESER_ACTOR_TASK_ARG = "(deserialize actor task arg)"


def data_lines(memstat_str):
    for line in memstat_str.split("\n"):
        if (not line or "---" in line or "===" in line or "Object ID" in line
                or "pid=" in line):
            continue
        yield line


def num_objects(memstat_str):
    n = 0
    for line in data_lines(memstat_str):
        n += 1
    return n


def count(memstat_str, substr):
    n = 0
    for line in memstat_str.split("\n"):
        if substr in line:
            n += 1
    return n


def test_driver_put_ref(ray_start_regular):
    info = memstat()
    assert num_objects(info) == 0, info
    x_id = ray.put("HI")
    info = memstat()
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, DRIVER_PID) == 1, info
    assert count(info, WORKER_PID) == 0, info
    del x_id
    info = memstat()
    assert num_objects(info) == 0, info


def test_worker_task_refs(ray_start_regular):
    @ray.remote
    def f(y):
        x_id = ray.put("HI")
        return memstat()

    x_id = f.remote(np.zeros(100000))
    info = ray.get(x_id)
    print(info)
    assert num_objects(info) == 4, info
    # Task argument plus task return ids.
    assert count(info, TASK_CALL_OBJ) == 2, info
    assert count(info, DRIVER_PID) == 1, info
    assert count(info, WORKER_PID) == 1, info
    assert count(info, LOCAL_REF) == 3, info
    assert count(info, PUT_OBJ) == 1, info
    assert count(info, DESER_TASK_ARG) == 1, info
    assert count(info, UNKNOWN_SIZE) == 1, info
    assert count(info, "test_memstat.py:f") == 1, info
    assert count(info, "test_memstat.py:test_worker_task_refs") == 2, info

    info = memstat()
    print(info)
    assert num_objects(info) == 1, info
    assert count(info, DRIVER_PID) == 1, info
    assert count(info, TASK_CALL_OBJ) == 1, info
    assert count(info, UNKNOWN_SIZE) == 0, info
    assert count(info, x_id.hex()) == 1, info

    del x_id
    info = memstat()
    assert num_objects(info) == 0, info


def test_actor_task_refs(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self):
            self.refs = []

        def f(self, x):
            self.refs.append(x)
            return memstat()

    def make_actor():
        return Actor.remote()

    actor = make_actor()
    x_id = actor.f.remote(np.zeros(100000))
    info = ray.get(x_id)
    print(info)
    assert num_objects(info) == 4, info
    # Actor handle, task argument id, task return id.
    assert count(info, ACTOR_TASK_CALL_OBJ) == 3, info
    assert count(info, DRIVER_PID) == 1, info
    assert count(info, WORKER_PID) == 1, info
    assert count(info, LOCAL_REF) == 2, info
    assert count(info, USED_BY_PENDING_TASK) == 2, info
    assert count(info, DESER_ACTOR_TASK_ARG) == 1, info
    assert count(info, "test_memstat.py:test_actor_task_refs") == 2, info
    assert count(info, "test_memstat.py:make_actor") == 1, info
    del x_id

    # These should accumulate in the actor.
    for _ in range(5):
        ray.get(actor.f.remote([ray.put(np.zeros(100000))]))
    info = memstat()
    print(info)
    assert count(info, DESER_ACTOR_TASK_ARG) == 5, info
    assert count(info, ACTOR_TASK_CALL_OBJ) == 1, info

    # Cleanup.
    del actor
    time.sleep(1)
    info = memstat()
    assert num_objects(info) == 0, info


def test_nested_object_refs(ray_start_regular):
    x_id = ray.put(np.zeros(100000))
    y_id = ray.put([x_id])
    z_id = ray.put([y_id])
    del x_id, y_id
    info = memstat()
    print(info)
    assert num_objects(info) == 3, info
    assert count(info, LOCAL_REF) == 1, info
    assert count(info, CAPTURED_IN_OBJECT) == 2, info


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
