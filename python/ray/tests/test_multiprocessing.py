from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest
import tempfile
import subprocess
from collections import defaultdict

import ray
from ray.experimental.multiprocessing import Pool, TimeoutError


@pytest.fixture
def cleanup_only():
    yield None
    ray.shutdown()
    subprocess.check_output(["ray", "stop"])
    if "RAY_ADDRESS" in os.environ:
        del os.environ["RAY_ADDRESS"]


@pytest.fixture
def pool():
    pool = Pool(processes=1)
    yield pool
    pool.terminate()
    ray.shutdown()


@pytest.fixture
def pool_4_processes():
    pool = Pool(processes=4)
    yield pool
    pool.terminate()
    ray.shutdown()


def test_initialize_ray(cleanup_only):
    def getpid(args):
        import os
        return os.getpid()

    def check_pool_size(pool, size):
        args = [tuple() for _ in range(size)]
        assert len(set(pool.map(getpid, args))) == size

    # Check that starting a pool starts ray if not initialized.
    pool = Pool(processes=2)
    assert ray.is_initialized()
    assert int(ray.state.cluster_resources()["CPU"]) == 2
    check_pool_size(pool, 2)
    ray.shutdown()

    # Check that starting a pool doesn't affect ray if there is a local
    # ray cluster running.
    ray.init(num_cpus=3)
    assert ray.is_initialized()
    pool = Pool(processes=2)
    assert int(ray.state.cluster_resources()["CPU"]) == 3
    check_pool_size(pool, 2)
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    ray.init(num_cpus=1)
    assert ray.is_initialized()
    with pytest.raises(ValueError):
        Pool(processes=2)
    assert int(ray.state.cluster_resources()["CPU"]) == 1
    ray.shutdown()

    # Use different numbers of CPUs to distinguish between starting a local
    # ray cluster and connecting to an existing one.
    init_cpus = 2
    start_cpus = 3

    # Start a ray cluster in the background.
    subprocess.check_output(
        ["ray", "start", "--head", "--num-cpus={}".format(start_cpus)])

    # Check that starting a pool still starts ray if RAY_ADDRESS not set.
    pool = Pool(processes=init_cpus)
    assert ray.is_initialized()
    assert int(ray.state.cluster_resources()["CPU"]) == init_cpus
    check_pool_size(pool, init_cpus)
    ray.shutdown()

    # Set RAY_ADDRESS, so pools should connect to the running ray cluster.
    os.environ["RAY_ADDRESS"] = "auto"

    # Check that starting a pool connects to a running ray cluster if
    # RAY_ADDRESS is set.
    pool = Pool()
    assert ray.is_initialized()
    assert int(ray.state.cluster_resources()["CPU"]) == start_cpus
    check_pool_size(pool, start_cpus)
    ray.shutdown()

    # Check that trying to start a pool on an existing ray cluster throws an
    # error if there aren't enough CPUs for the number of processes.
    with pytest.raises(Exception):
        Pool(processes=start_cpus + 1)
    assert int(ray.state.cluster_resources()["CPU"]) == start_cpus
    ray.shutdown()

    # Clean up the background ray cluster.
    subprocess.check_output(["ray", "stop"])


def test_initializer(cleanup_only):
    def init(dirname):
        with open(os.path.join(dirname, str(os.getpid())), "w") as f:
            print("hello", file=f)

    with tempfile.TemporaryDirectory() as dirname:
        num_processes = 4
        pool = Pool(
            processes=num_processes, initializer=init, initargs=(dirname, ))

        assert len(os.listdir(dirname)) == 4
        pool.terminate()


@pytest.mark.skip(reason="Modifying globals in initializer not working.")
def test_initializer_globals(cleanup_only):
    def init(arg1, arg2):
        global x
        x = arg1 + arg2

    pool = Pool(processes=4, initializer=init, initargs=(1, 2))

    def get(i):
        return x

    for result in pool.map(get, range(100)):
        assert result == 3

    pool.terminate()
    ray.shutdown()


def test_close(pool_4_processes):
    def f(object_id):
        return ray.get(object_id)

    object_id = ray.ObjectID.from_random()
    result = pool_4_processes.map_async(f, [object_id for _ in range(4)])
    assert not result.ready()
    pool_4_processes.close()
    assert not result.ready()

    # Fulfill the object_id, causing the head of line tasks to finish.
    ray.worker.global_worker.put_object("hello", object_id=object_id)
    pool_4_processes.join()

    # close() shouldn't interrupt pending tasks, so check that they succeeded.
    assert result.ready()
    assert result.successful()
    assert result.get() == ["hello"] * 4


def test_terminate(pool_4_processes):
    def f(object_id):
        return ray.get(object_id)

    object_id = ray.ObjectID.from_random()
    result = pool_4_processes.map_async(f, [object_id for _ in range(4)])
    assert not result.ready()
    pool_4_processes.terminate()

    # terminate() should interrupt pending tasks, so check that join() returns
    # even though the tasks should be blocked forever.
    pool_4_processes.join()
    assert result.ready()
    assert not result.successful()


def test_apply(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply(f, (1, 2), {"kwarg2": 3}) == 1
    with pytest.raises(AssertionError):
        pool.apply(f, (
            2,
            2,
        ), {"kwarg2": 3})
    with pytest.raises(Exception):
        pool.apply(f, (1, ))
    with pytest.raises(Exception):
        pool.apply(f, (1, 2), {"kwarg1": 3})


def test_apply_async(pool):
    def f(arg1, arg2, kwarg1=None, kwarg2=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg1 is None
        assert kwarg2 == 3
        return 1

    assert pool.apply_async(f, (1, 2), {"kwarg2": 3}).get() == 1
    with pytest.raises(AssertionError):
        pool.apply_async(f, (
            2,
            2,
        ), {
            "kwarg2": 3
        }).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, )).get()
    with pytest.raises(Exception):
        pool.apply_async(f, (1, 2), {"kwarg1": 3}).get()

    # Won't return until the input ObjectID is fulfilled.
    def ten_over(input):
        return 10 / ray.get(input[0])

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    result = pool.apply_async(ten_over, ([object_id], ))
    result.wait(timeout=0.01)
    assert not result.ready()
    with pytest.raises(TimeoutError):
        result.get(timeout=0.01)

    # Fulfill the ObjectID.
    ray.worker.global_worker.put_object(10, object_id=object_id)
    result.wait(timeout=10)
    assert result.ready()
    assert result.successful()
    assert result.get() == 1

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    result = pool.apply_async(ten_over, ([object_id], ))
    with pytest.raises(ValueError, match="not ready"):
        result.successful()

    # Fulfill the ObjectID with 0, causing the task to fail (divide by zero).
    ray.worker.global_worker.put_object(0, object_id=object_id)
    result.wait(timeout=10)
    assert result.ready()
    assert not result.successful()
    with pytest.raises(ZeroDivisionError):
        result.get()


def test_map(pool_4_processes):
    def f(index):
        import os
        return index, os.getpid()

    results = pool_4_processes.map(f, range(1000))
    assert len(results) == 1000

    pid_counts = defaultdict(int)
    for i, (index, pid) in enumerate(results):
        assert i == index
        pid_counts[pid] += 1

    # Check that the functions are spread somewhat evenly.
    for count in pid_counts.values():
        assert count > 100

    def bad_func(args):
        raise Exception("test_map failure")

    with pytest.raises(Exception, match="test_map failure"):
        pool_4_processes.map(bad_func, range(100))


def test_map_async(pool_4_processes):
    def f(args):
        import os
        index = args[0]
        ray.get(args[1])
        return index, os.getpid()

    # Generate a random ObjectID that will be fulfilled later.
    object_id = ray.ObjectID.from_random()
    async_result = pool_4_processes.map_async(
        f, [(i, object_id) for i in range(1000)])
    assert not async_result.ready()
    with pytest.raises(TimeoutError):
        async_result.get(timeout=0.01)
    async_result.wait(timeout=0.01)

    # Fulfill the object ID, finishing the tasks.
    ray.worker.global_worker.put_object(0, object_id=object_id)
    async_result.wait(timeout=10)
    assert async_result.ready()
    assert async_result.successful()

    results = async_result.get()
    assert len(results) == 1000

    pid_counts = defaultdict(int)
    for i, (index, pid) in enumerate(results):
        assert i == index
        pid_counts[pid] += 1

    # Check that the functions are spread somewhat evenly.
    for count in pid_counts.values():
        assert count > 100

    def bad_func(index):
        if index == 50:
            raise Exception("test_map_async failure")

    async_result = pool_4_processes.map_async(bad_func, range(100))
    async_result.wait(10)
    assert async_result.ready()
    assert not async_result.successful()

    with pytest.raises(Exception, match="test_map_async failure"):
        async_result.get()


def test_starmap(pool_4_processes):
    pass


def test_starmap_async(pool_4_processes):
    pass


def imap(pool_4_processes):
    pass


def imap_unordered(pool_4_processes):
    pass
