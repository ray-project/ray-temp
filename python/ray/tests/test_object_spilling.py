import copy
import json
import random
import platform
import sys
from collections import defaultdict

import numpy as np
import pytest
import ray
from ray.tests.conftest import (file_system_object_spilling_config,
                                mock_distributed_fs_object_spilling_config)
from ray.external_storage import (create_url_with_offset,
                                  parse_url_with_offset)
from ray.test_utils import wait_for_condition
from ray.internal.internal_api import memory_summary


# -- Smart open param --
bucket_name = "object-spilling-test"

# -- File system param --
spill_local_path = "/tmp/spill"

# -- Spilling configs --
file_system_object_spilling_config = {
    "type": "filesystem",
    "params": {
        "directory_path": spill_local_path
    }
}

unstable_object_spilling_config = {
    "type": "unstable_fs",
    "params": {
        "directory_path": spill_local_path,
    }
}

# Since we have differet protocol for a local external storage (e.g., fs)
# and distributed external storage (e.g., S3), we need to test both cases.
# This mocks the distributed fs with cluster utils.
mock_distributed_fs_object_spilling_config = {
    "type": "mock_distributed_fs",
    "params": {
        "directory_path": spill_local_path
    }
}

smart_open_object_spilling_config = {
    "type": "smart_open",
    "params": {
        "uri": f"s3://{bucket_name}/"
    }
}


def create_object_spilling_config(request, tmp_path):
    temp_folder = tmp_path / "spill"
    temp_folder.mkdir()
    if (request.param["type"] == "filesystem"
            or request.param["type"] == "mock_distributed_fs"):
        request.param["params"]["directory_path"] = str(temp_folder)
    return json.dumps(request.param), temp_folder


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        # TODO(sang): Add a mock dependency to test S3.
        # smart_open_object_spilling_config,
    ])
def object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function", params=[
        unstable_object_spilling_config,
    ])
def unstable_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(scope="function", params=[unstable_object_spilling_config])
def unstable_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


@pytest.fixture(
    scope="function",
    params=[
        file_system_object_spilling_config,
        mock_distributed_fs_object_spilling_config
    ])
def multi_node_object_spilling_config(request, tmp_path):
    yield create_object_spilling_config(request, tmp_path)


def run_basic_workload():
    """Run the workload that requires spilling."""
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    refs = []
    refs.append([ray.put(arr) for _ in range(2)])
    ray.get(ray.put(arr))


def is_dir_empty(temp_folder,
                 append_path=ray.ray_constants.DEFAULT_OBJECT_PREFIX):
    # append_path is used because the file based spilling will append
    # new directory path.
    num_files = 0
    temp_folder = temp_folder / append_path
    for path in temp_folder.iterdir():
        num_files += 1
    return num_files == 0


def assert_no_thrashing(address):
    state = ray.state.GlobalState()
    state._initialize_global_state(address,
                                   ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    summary = memory_summary(address=address, stats_only=True)
    restored_bytes = 0
    consumed_bytes = 0

    for line in summary.split("\n"):
        if "Restored" in line:
            restored_bytes = int(line.split(" ")[1])
        if "consumed" in line:
            consumed_bytes = int(line.split(" ")[-2])
    assert consumed_bytes >= restored_bytes, (
        f"consumed: {consumed_bytes}, restored: {restored_bytes}")


def test_invalid_config_raises_exception(shutdown_only):
    # Make sure ray.init raises an exception before
    # it starts processes when invalid object spilling
    # config is given.
    with pytest.raises(ValueError):
        ray.init(_system_config={
            "object_spilling_config": json.dumps({
                "type": "abc"
            }),
        })

    with pytest.raises(Exception):
        copied_config = copy.deepcopy(file_system_object_spilling_config)
        # Add invalid params to the config.
        copied_config["params"].update({"random_arg": "abc"})
        ray.init(_system_config={
            "object_spilling_config": json.dumps(copied_config),
        })


def test_url_generation_and_parse():
    url = "s3://abc/def/ray_good"
    offset = 10
    size = 30
    url_with_offset = create_url_with_offset(url=url, offset=offset, size=size)
    parsed_result = parse_url_with_offset(url_with_offset)
    assert parsed_result.base_url == url
    assert parsed_result.offset == offset
    assert parsed_result.size == size


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_default_config(shutdown_only):
    ray.init(num_cpus=0, object_store_memory=75 * 1024 * 1024)
    # Make sure the object spilling configuration is properly set.
    config = json.loads(
        ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == "filesystem"
    assert (config["params"]["directory_path"] ==
            ray.worker._global_node._session_dir)
    # Make sure the basic workload can succeed.
    run_basic_workload()
    ray.shutdown()

    # Make sure config is not initalized if spilling is not enabled..
    ray.init(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": False,
            "object_store_full_delay_ms": 100
        })
    assert "object_spilling_config" not in ray.worker._global_node._config
    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        run_basic_workload()
    ray.shutdown()

    # Make sure when we use a different config, it is reflected.
    ray.init(
        num_cpus=0,
        _system_config={
            "object_spilling_config": (
                json.dumps(mock_distributed_fs_object_spilling_config))
        })
    config = json.loads(
        ray.worker._global_node._config["object_spilling_config"])
    assert config["type"] == "mock_distributed_fs"


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_default_config_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(cluster.address)
    worker_nodes = []
    worker_nodes.append(
        cluster.add_node(num_cpus=1, object_store_memory=75 * 1024 * 1024))
    cluster.wait_for_nodes()

    # Run the basic spilling workload on both
    # worker nodes and make sure they are working.
    @ray.remote
    def task():
        arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
        refs = []
        refs.append([ray.put(arr) for _ in range(2)])
        ray.get(ray.put(arr))

    ray.get([task.remote() for _ in range(2)])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spilling_not_done_for_pinned_object(object_spilling_config,
                                             shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, temp_folder = object_spilling_config
    address = ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        })
    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    ref = ray.get(ray.put(arr))  # noqa
    # Since the ref exists, it should raise OOM.
    with pytest.raises(ray.exceptions.ObjectStoreFullError):
        ref2 = ray.put(arr)  # noqa

    wait_for_condition(lambda: is_dir_empty(temp_folder))
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_remote_object(ray_start_cluster,
                             multi_node_object_spilling_config):
    cluster = ray_start_cluster
    object_spilling_config, _ = multi_node_object_spilling_config
    cluster.add_node(
        num_cpus=0,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "max_io_workers": 4,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        })
    ray.init(address=cluster.address)
    cluster.add_node(object_store_memory=75 * 1024 * 1024)
    cluster.wait_for_nodes()

    @ray.remote
    def put():
        return np.random.rand(5 * 1024 * 1024)  # 40 MB data

    @ray.remote
    def depends(arg):
        return

    ref = put.remote()
    copy = np.copy(ray.get(ref))
    # Evict local copy.
    ray.put(np.random.rand(5 * 1024 * 1024))  # 40 MB data
    # Remote copy should cause first remote object to get spilled.
    ray.get(put.remote())

    sample = ray.get(ref)
    assert np.array_equal(sample, copy)
    # Evict the spilled object.
    del sample
    ray.get(put.remote())
    ray.put(np.random.rand(5 * 1024 * 1024))  # 40 MB data

    # Test passing the spilled object as an arg to another task.
    ray.get(depends.remote(ref))
    assert_no_thrashing(cluster.address)


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_objects_automatically(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0
        })
    replay_buffer = []
    solution_buffer = []
    buffer_length = 100

    # Create objects of more than 800 MiB.
    for _ in range(buffer_length):
        ref = None
        while ref is None:
            multiplier = random.choice([1, 2, 3])
            arr = np.random.rand(multiplier * 1024 * 1024)
            ref = ray.put(arr)
            replay_buffer.append(ref)
            solution_buffer.append(arr)
    print("spill done.")
    # randomly sample objects
    for _ in range(1000):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_unstable_spill_objects_automatically(unstable_spilling_config,
                                              shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = unstable_spilling_config
    address = ray.init(
        num_cpus=1,
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 4,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0
        })
    replay_buffer = []
    solution_buffer = []
    buffer_length = 100

    # Create objects of more than 800 MiB.
    for _ in range(buffer_length):
        ref = None
        while ref is None:
            multiplier = random.choice([1, 2, 3])
            arr = np.random.rand(multiplier * 1024 * 1024)
            ref = ray.put(arr)
            replay_buffer.append(ref)
            solution_buffer.append(arr)
    print("spill done.")
    # randomly sample objects
    for _ in range(1000):
        index = random.choice(list(range(buffer_length)))
        ref = replay_buffer[index]
        solution = solution_buffer[index]
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, solution)
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() in ["Windows", "Darwin"], reason="Failing on Windows.")
def test_spill_stats(object_spilling_config, shutdown_only):
    # Limit our object store to 75 MiB of memory.
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
        num_cpus=1,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "max_io_workers": 100,
            "min_spilling_size": 1,
            "object_spilling_config": object_spilling_config
        },
    )

    @ray.remote
    def f():
        return np.zeros(50 * 1024 * 1024, dtype=np.uint8)

    ids = []
    for _ in range(4):
        x = f.remote()
        ids.append(x)

    while ids:
        print(ray.get(ids.pop()))

    x_id = f.remote()  # noqa
    ray.get(x_id)
    s = memory_summary(address=address["redis_address"], stats_only=True)
    assert "Plasma memory usage 50 MiB, 1 objects, 50.0% full" in s, s
    assert "Spilled 200 MiB, 4 objects" in s, s
    assert "Restored 150 MiB, 3 objects" in s, s

    # Test if consumed bytes are correctly calculated.
    obj = ray.put(np.zeros(30 * 1024 * 1024, dtype=np.uint8))

    @ray.remote
    def func_with_ref(obj):
        return True

    ray.get(func_with_ref.remote(obj))

    s = memory_summary(address=address["redis_address"], stats_only=True)
    # 50MB * 5 references + 30MB used for task execution.
    assert "Objects consumed by Ray tasks: 280 MiB." in s, s
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_during_get(object_spilling_config, shutdown_only):
    object_spilling_config, _ = object_spilling_config
    address = ray.init(
        num_cpus=4,
        object_store_memory=100 * 1024 * 1024,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "max_io_workers": 1,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )

    @ray.remote
    def f():
        return np.zeros(10 * 1024 * 1024)

    ids = []
    for i in range(10):
        x = f.remote()
        print(i, x)
        ids.append(x)

    # Concurrent gets, which require restoring from external storage, while
    # objects are being created.
    for x in ids:
        print(ray.get(x).shape)
    assert_no_thrashing(address["redis_address"])


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_spill_deadlock(object_spilling_config, shutdown_only):
    object_spilling_config, _ = object_spilling_config
    # Limit our object store to 75 MiB of memory.
    address = ray.init(
        object_store_memory=75 * 1024 * 1024,
        _system_config={
            "max_io_workers": 1,
            "automatic_object_spilling_enabled": True,
            "object_store_full_delay_ms": 100,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        })
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    # Create objects of more than 400 MiB.
    for _ in range(50):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)
        # This is doing random sampling with 50% prob.
        if random.randint(0, 9) < 5:
            for _ in range(5):
                ref = random.choice(replay_buffer)
                sample = ray.get(ref, timeout=0)
                assert np.array_equal(sample, arr)
    assert_no_thrashing(address["redis_address"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
