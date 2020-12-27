import pytest
from ray.experimental.client.ray_client_helpers import ray_start_client_server
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.test_utils import wait_for_condition
from ray.exceptions import TaskCancelledError
from ray.exceptions import RayTaskError
from ray.exceptions import WorkerCrashedError
from ray.exceptions import ObjectLostError
from ray.exceptions import GetTimeoutError


def valid_exceptions(use_force):
    if use_force:
        return (RayTaskError, TaskCancelledError, WorkerCrashedError,
                ObjectLostError)
    else:
        return (RayTaskError, TaskCancelledError)


def _all_actors_dead(ray):
    import ray as real_ray

    def _all_actors_dead_internal():
        return all(actor["State"] == real_ray.gcs_utils.ActorTableData.DEAD
                   for actor in list(real_ray.actors().values()))

    return _all_actors_dead_internal


def test_kill_actor_immediately_after_creation(ray_start_regular):
    with ray_start_client_server() as ray:

        @ray.remote
        class A:
            pass

        a = A.remote()
        b = A.remote()

        ray.kill(a)
        ray.kill(b)
        wait_for_condition(_all_actors_dead(ray), timeout=10)


@pytest.mark.parametrize("use_force", [True, False])
def test_cancel_chain(ray_start_regular, use_force):
    with ray_start_client_server() as ray:
        SignalActor = create_remote_signal_actor(ray)
        signaler = SignalActor.remote()

        @ray.remote
        def wait_for(t):
            return ray.get(t[0])

        obj1 = wait_for.remote([signaler.wait.remote()])
        obj2 = wait_for.remote([obj1])
        obj3 = wait_for.remote([obj2])
        obj4 = wait_for.remote([obj3])

        assert len(ray.wait([obj1], timeout=.1)[0]) == 0
        ray.cancel(obj1, force=use_force)
        for ob in [obj1, obj2, obj3, obj4]:
            with pytest.raises(valid_exceptions(use_force)):
                ray.get(ob)

        signaler2 = SignalActor.remote()
        obj1 = wait_for.remote([signaler2.wait.remote()])
        obj2 = wait_for.remote([obj1])
        obj3 = wait_for.remote([obj2])
        obj4 = wait_for.remote([obj3])

        assert len(ray.wait([obj3], timeout=.1)[0]) == 0
        ray.cancel(obj3, force=use_force)
        for ob in [obj3, obj4]:
            with pytest.raises(valid_exceptions(use_force)):
                ray.get(ob)

        with pytest.raises(GetTimeoutError):
            ray.get(obj1, timeout=.1)

        with pytest.raises(GetTimeoutError):
            ray.get(obj2, timeout=.1)

        signaler2.send.remote()
        ray.get(obj1)
