import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None

import ray
import ray.exceptions
import ray.experimental.no_return
import ray.worker


@pytest.fixture
def ray_start():
    # Start the Ray processes.
    ray.init(num_cpus=1)
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_set_single_output(ray_start):
    @ray.remote
    def f():
        return_object_ids = ray.worker.global_worker._current_task.returns()
        ray.worker.global_worker.put_object(return_object_ids[0], 123)
        return ray.experimental.no_return.NoReturn

    assert ray.get(f.remote()) == 123


@pytest.mark.parametrize("set_out0", [True, False])
@pytest.mark.parametrize("set_out1", [True, False])
@pytest.mark.parametrize("set_out2", [True, False])
def test_set_multiple_outputs(ray_start, set_out0, set_out1, set_out2):
    @ray.remote(num_return_vals=3)
    def f(set_out0, set_out1, set_out3):
        returns = []
        return_object_ids = ray.worker.global_worker._current_task.returns()
        for i, set_out in enumerate([set_out0, set_out1, set_out2]):
            if set_out:
                ray.worker.global_worker.put_object(return_object_ids[i], True)
                returns.append(ray.experimental.no_return.NoReturn)
            else:
                returns.append(False)
        return tuple(returns)

    result_object_ids = f.remote(set_out0, set_out1, set_out2)
    assert ray.get(result_object_ids) == [set_out0, set_out1, set_out2]


def test_exception(ray_start):
    @ray.remote(num_return_vals=2)
    def f():
        return_object_ids = ray.worker.global_worker._current_task.returns()
        # The first return value is successfully stored in the object store
        ray.worker.global_worker.put_object(return_object_ids[0], 123)
        raise Exception
        # The exception is stored at the second return objcet ID.
        return ray.experimental.no_return.NoReturn, 456

    object_id, exception_id = f.remote()

    assert ray.get(object_id) == 123
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(exception_id)


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(5)
def test_no_set_and_no_return(ray_start):
    @ray.remote
    def f():
        return ray.experimental.no_return.NoReturn

    object_id = f.remote()
    assert ray.get(object_id) is ray.experimental.no_return.NoReturn
