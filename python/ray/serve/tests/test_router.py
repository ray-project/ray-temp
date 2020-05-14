import asyncio

import pytest
import ray

from ray.serve.router import Router
from ray.serve.request_params import RequestMetadata
from ray.serve.utils import get_random_letters

pytestmark = pytest.mark.asyncio


def mock_task_runner():
    @ray.remote(num_cpus=0)
    class TaskRunnerMock:
        def __init__(self):
            self.query = None
            self.queries = []

        async def handle_request(self, request):
            self.query = request
            self.queries.append(request)
            return "DONE"

        def get_recent_call(self):
            return self.query

        def get_all_calls(self):
            return self.queries

        def ready(self):
            pass

    return TaskRunnerMock.remote()


@pytest.fixture
def task_runner_mock_actor():
    yield mock_task_runner()


async def test_single_prod_cons_queue(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    q.set_traffic.remote("svc", {"backend-single-prod": 1.0})
    q.add_new_worker.remote("backend-single-prod", "replica-1",
                            task_runner_mock_actor)

    # Make sure we get the request result back
    result = await q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    assert result == "DONE"

    # Make sure it's the right request
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 1
    assert got_work.request_kwargs == {}


async def test_slo(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()
    await q.set_traffic.remote("svc", {"backend-slo": 1.0})

    all_request_sent = []
    for i in range(10):
        slo_ms = 1000 - 100 * i
        all_request_sent.append(
            q.enqueue_request.remote(
                RequestMetadata("svc", None, relative_slo_ms=slo_ms), i))

    await q.add_new_worker.remote("backend-slo", "replica-1",
                                  task_runner_mock_actor)

    await asyncio.gather(*all_request_sent)

    i_should_be = 9
    all_calls = await task_runner_mock_actor.get_all_calls.remote()
    all_calls = all_calls[-10:]
    for call in all_calls:
        assert call.request_args[0] == i_should_be
        i_should_be -= 1


async def test_alter_backend(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()

    await q.set_traffic.remote("svc", {"backend-alter": 1})
    await q.add_new_worker.remote("backend-alter", "replica-1",
                                  task_runner_mock_actor)
    await q.enqueue_request.remote(RequestMetadata("svc", None), 1)
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 1

    await q.set_traffic.remote("svc", {"backend-alter-2": 1})
    await q.add_new_worker.remote("backend-alter-2", "replica-1",
                                  task_runner_mock_actor)
    await q.enqueue_request.remote(RequestMetadata("svc", None), 2)
    got_work = await task_runner_mock_actor.get_recent_call.remote()
    assert got_work.request_args[0] == 2


async def test_split_traffic_random(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()

    await q.set_traffic.remote("svc", {
        "backend-split": 0.5,
        "backend-split-2": 0.5
    })
    runner_1, runner_2 = [mock_task_runner() for _ in range(2)]
    await q.add_new_worker.remote("backend-split", "replica-1", runner_1)
    await q.add_new_worker.remote("backend-split-2", "replica-1", runner_2)

    # assume 50% split, the probability of all 20 requests goes to a
    # single queue is 0.5^20 ~ 1-6
    for _ in range(20):
        await q.enqueue_request.remote(RequestMetadata("svc", None), 1)

    got_work = [
        await runner.get_recent_call.remote()
        for runner in (runner_1, runner_2)
    ]
    assert [g.request_args[0] for g in got_work] == [1, 1]


async def test_queue_remove_replicas(serve_instance):
    class TestRouter(Router):
        def worker_queue_size(self, backend):
            return self.worker_queues["backend-remove"].qsize()

    temp_actor = mock_task_runner()
    q = ray.remote(TestRouter).remote()
    await q.add_new_worker.remote("backend-remove", "replica-1", temp_actor)
    await q.remove_worker.remote("backend-remove", "replica-1")
    assert ray.get(q.worker_queue_size.remote("backend")) == 0


async def test_shard_key(serve_instance, task_runner_mock_actor):
    q = ray.remote(Router).remote()

    await q.set_traffic.remote("svc", {
        "backend-split": 0.5,
        "backend-split-2": 0.5
    })
    runner_1, runner_2 = [mock_task_runner() for _ in range(2)]
    await q.add_new_worker.remote("backend-split", "replica-1", runner_1)
    await q.add_new_worker.remote("backend-split-2", "replica-1", runner_2)

    # assume 50% split, the probability of all 20 requests goes to a
    # single queue is 0.5^20 ~ 1-6
    for _ in range(20):
        await q.enqueue_request.remote(
            RequestMetadata("svc", None, shard_key=get_random_letters()), 1)

    got_work = [
        await runner.get_recent_call.remote()
        for runner in (runner_1, runner_2)
    ]
    assert [g.request_args[0] for g in got_work] == [1, 1]
