import pytest
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import time

import ray
import ray.ray_constants
import ray.test_utils
from ray.test_utils import new_scheduler_enabled

from ray._raylet import GlobalStateAccessor


# TODO(rliaw): The proper way to do this is to have the pytest config setup.
@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(10)
def test_replenish_resources(ray_start_regular):
    cluster_resources = ray.cluster_resources()
    available_resources = ray.available_resources()
    assert cluster_resources == available_resources

    @ray.remote
    def cpu_task():
        pass

    ray.get(cpu_task.remote())
    resources_reset = False

    while not resources_reset:
        available_resources = ray.available_resources()
        resources_reset = (cluster_resources == available_resources)
    assert resources_reset


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(10)
def test_uses_resources(ray_start_regular):
    cluster_resources = ray.cluster_resources()

    @ray.remote
    def cpu_task():
        time.sleep(1)

    cpu_task.remote()
    resource_used = False

    while not resource_used:
        available_resources = ray.available_resources()
        resource_used = available_resources.get(
            "CPU", 0) == cluster_resources.get("CPU", 0) - 1

    assert resource_used


@pytest.mark.skipif(
    pytest_timeout is None,
    reason="Timeout package not installed; skipping test that may hang.")
@pytest.mark.timeout(120)
def test_add_remove_cluster_resources(ray_start_cluster_head):
    """Tests that Global State API is consistent with actual cluster."""
    cluster = ray_start_cluster_head
    assert ray.cluster_resources()["CPU"] == 1
    nodes = []
    nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 2

    cluster.remove_node(nodes.pop())
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 6


def test_global_state_actor_table(ray_start_regular):
    @ray.remote
    class Actor:
        def ready(self):
            pass

    # actor table should be empty at first
    assert len(ray.actors()) == 0

    # actor table should contain only one entry
    a = Actor.remote()
    ray.get(a.ready.remote())
    assert len(ray.actors()) == 1

    # actor table should contain only this entry
    # even when the actor goes out of scope
    del a

    def get_state():
        return list(ray.actors().values())[0]["State"]

    dead_state = ray.gcs_utils.ActorTableData.DEAD
    for _ in range(10):
        if get_state() == dead_state:
            break
        else:
            time.sleep(0.5)
    assert get_state() == dead_state


def test_global_state_worker_table(ray_start_regular):

    # Get worker table from gcs.
    workers_data = ray.state.workers()

    assert len(workers_data) == 1


def test_global_state_actor_entry(ray_start_regular):
    @ray.remote
    class Actor:
        def ready(self):
            pass

    # actor table should be empty at first
    assert len(ray.actors()) == 0

    a = Actor.remote()
    b = Actor.remote()
    ray.get(a.ready.remote())
    ray.get(b.ready.remote())
    assert len(ray.actors()) == 2
    a_actor_id = a._actor_id.hex()
    b_actor_id = b._actor_id.hex()
    assert ray.actors(actor_id=a_actor_id)["ActorID"] == a_actor_id
    assert ray.actors(
        actor_id=a_actor_id)["State"] == ray.gcs_utils.ActorTableData.ALIVE
    assert ray.actors(actor_id=b_actor_id)["ActorID"] == b_actor_id
    assert ray.actors(
        actor_id=b_actor_id)["State"] == ray.gcs_utils.ActorTableData.ALIVE


@pytest.mark.parametrize("max_shapes", [0, 2, -1])
def test_load_report(shutdown_only, max_shapes):
    resource1 = "A"
    resource2 = "B"
    cluster = ray.init(
        num_cpus=1,
        resources={resource1: 1},
        _system_config={
            "max_resource_shapes_per_load_report": max_shapes,
        })
    global_state_accessor = GlobalStateAccessor(
        cluster["redis_address"], ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    global_state_accessor.connect()

    @ray.remote
    def sleep():
        time.sleep(1000)

    sleep.remote()
    for _ in range(3):
        sleep.remote()
        sleep.options(resources={resource1: 1}).remote()
        sleep.options(resources={resource2: 1}).remote()

    class Checker:
        def __init__(self):
            self.report = None

        def check_load_report(self):
            message = global_state_accessor.get_all_heartbeat()
            if message is None:
                return False

            heartbeat = ray.gcs_utils.HeartbeatBatchTableData.FromString(
                message)
            self.report = heartbeat.resource_load_by_shape.resource_demands
            if max_shapes == 0:
                return True
            elif max_shapes == 2:
                return len(self.report) >= 2
            else:
                return len(self.report) >= 3

    # Wait for load information to arrive.
    checker = Checker()
    ray.test_utils.wait_for_condition(checker.check_load_report)

    # Check that we respect the max shapes limit.
    if max_shapes != -1:
        assert len(checker.report) <= max_shapes

    print(checker.report)

    if max_shapes > 0:
        # Check that we always include the 1-CPU resource shape.
        one_cpu_shape = {"CPU": 1}
        one_cpu_found = False
        for demand in checker.report:
            if demand.shape == one_cpu_shape:
                one_cpu_found = True
        assert one_cpu_found

        # Check that we differentiate between infeasible and ready tasks.
        for demand in checker.report:
            if resource2 in demand.shape:
                assert demand.num_infeasible_requests_queued > 0
                assert demand.num_ready_requests_queued == 0
            else:
                assert demand.num_ready_requests_queued > 0
                assert demand.num_infeasible_requests_queued == 0
    global_state_accessor.disconnect()


@pytest.mark.skipif(new_scheduler_enabled(), reason="broken")
def test_placement_group_load_report(ray_start_cluster):
    cluster = ray_start_cluster
    # Add a head node that doesn't have gpu resource.
    cluster.add_node(num_cpus=4)
    ray.init(address=cluster.address)
    global_state_accessor = GlobalStateAccessor(
        cluster.address, ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    global_state_accessor.connect()

    class PgLoadChecker:
        def nothing_is_ready(self):
            heartbeat = self._read_heartbeat()
            if not heartbeat:
                return False
            if heartbeat.HasField("placement_group_load"):
                pg_load = heartbeat.placement_group_load
                return len(pg_load.placement_group_data) == 2
            return False

        def only_first_one_ready(self):
            heartbeat = self._read_heartbeat()
            if not heartbeat:
                return False
            if heartbeat.HasField("placement_group_load"):
                pg_load = heartbeat.placement_group_load
                return len(pg_load.placement_group_data) == 1
            return False

        def two_infeasible_pg(self):
            heartbeat = self._read_heartbeat()
            if not heartbeat:
                return False
            if heartbeat.HasField("placement_group_load"):
                pg_load = heartbeat.placement_group_load
                return len(pg_load.placement_group_data) == 2
            return False

        def _read_heartbeat(self):
            message = global_state_accessor.get_all_heartbeat()
            if message is None:
                return False

            heartbeat = ray.gcs_utils.HeartbeatBatchTableData.FromString(
                message)
            return heartbeat

    checker = PgLoadChecker()

    # Create 2 placement groups that are infeasible.
    pg_feasible = ray.util.placement_group([{"A": 1}])
    pg_infeasible = ray.util.placement_group([{"B": 1}])
    _, unready = ray.wait(
        [pg_feasible.ready(), pg_infeasible.ready()], timeout=0)
    assert len(unready) == 2
    ray.test_utils.wait_for_condition(checker.nothing_is_ready)

    # Add a node that makes pg feasible. Make sure load include this change.
    cluster.add_node(resources={"A": 1})
    ray.get(pg_feasible.ready())
    ray.test_utils.wait_for_condition(checker.only_first_one_ready)
    # Create one more infeasible pg and make sure load is properly updated.
    pg_infeasible_second = ray.util.placement_group([{"C": 1}])
    _, unready = ray.wait([pg_infeasible_second.ready()], timeout=0)
    assert len(unready) == 1
    ray.test_utils.wait_for_condition(checker.two_infeasible_pg)
    global_state_accessor.disconnect()


@pytest.mark.skipif(new_scheduler_enabled(), reason="broken")
def test_backlog_report(shutdown_only):
    cluster = ray.init(
        num_cpus=1, _system_config={
            "report_worker_backlog": True,
        })
    global_state_accessor = GlobalStateAccessor(
        cluster["redis_address"], ray.ray_constants.REDIS_DEFAULT_PASSWORD)
    global_state_accessor.connect()

    @ray.remote(num_cpus=1)
    def foo(x):
        print(".")
        time.sleep(x)
        return None

    def backlog_size_set():
        message = global_state_accessor.get_all_heartbeat()
        if message is None:
            return False

        heartbeat = ray.gcs_utils.HeartbeatBatchTableData.FromString(message)
        aggregate_resource_load = \
            heartbeat.resource_load_by_shape.resource_demands
        if len(aggregate_resource_load) == 1:
            backlog_size = aggregate_resource_load[0].backlog_size
            print(backlog_size)
            # Ideally we'd want to assert backlog_size == 8, but guaranteeing
            # the order the order that submissions will occur is too
            # hard/flaky.
            return backlog_size > 0
        return False

    # We want this first task to finish
    refs = [foo.remote(0.5)]
    # These tasks should all start _before_ the first one finishes.
    refs.extend([foo.remote(1000) for _ in range(9)])
    # Now there's 1 request running, 1 queued in the raylet, and 8 queued in
    # the worker backlog.

    ray.get(refs[0])
    # First request finishes, second request is now running, third lease
    # request is sent to the raylet with backlog=7

    ray.test_utils.wait_for_condition(backlog_size_set, timeout=2)
    global_state_accessor.disconnect()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
