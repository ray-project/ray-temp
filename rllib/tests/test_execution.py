import pytest
import time
import gym
import queue

from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.rollout_ops import ParallelRollouts, AsyncGradients, \
    ConcatBatches
from ray.rllib.execution.train_ops import TrainOneStep, ComputeGradients, \
    AverageGradients
from ray.tests.conftest import ray_start_regular_shared
from ray.util.iter import LocalIterator, from_range
from ray.util.iter_metrics import SharedMetrics
import ray


def iter_list(values):
    return LocalIterator(lambda _: values, SharedMetrics())


def make_workers(n):
    local = RolloutWorker(
        env_creator=lambda _: gym.make("CartPole-v0"),
        policy=PPOTFPolicy,
        rollout_fragment_length=100)
    remotes = [
        RolloutWorker.as_remote().remote(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy=PPOTFPolicy,
            rollout_fragment_length=100)
        for _ in range(n)
    ]
    workers = WorkerSet._from_existing(local, remotes)
    return workers


def test_concurrently(ray_start_regular_shared):
    a = iter_list([1, 2, 3])
    b = iter_list([4, 5, 6])
    c = Concurrently([a, b], mode="round_robin")
    assert c.take(6) == [1, 4, 2, 5, 3, 6]

    a = iter_list([1, 2, 3])
    b = iter_list([4, 5, 6])
    c = Concurrently([a, b], mode="async")
    assert c.take(6) == [1, 2, 3, 4, 5, 6]


def test_enqueue_dequeue(ray_start_regular_shared):
    a = iter_list([1, 2, 3])
    q = queue.Queue(100)
    a.for_each(Enqueue(q)).take(3)
    assert q.qsize() == 3
    assert q.get_nowait() == 1
    assert q.get_nowait() == 2
    assert q.get_nowait() == 3

    q.put("a")
    q.put("b")
    q.put("c")
    a = Dequeue(q)
    assert a.take(3) == ["a", "b", "c"]


def test_metrics(ray_start_regular_shared):
    workers = make_workers(1)
    workers.foreach_worker(lambda w: w.sample())
    a = from_range(10, repeat=True).gather_sync()
    b = StandardMetricsReporting(a, workers, {
        "min_iter_time_s": 2.5,
        "metrics_smoothing_episodes": 10,
        "collect_metrics_timeout": 10,
    })

    start = time.time()
    res1 = next(b)
    assert res1["episode_reward_mean"] > 0, res1
    res2 = next(b)
    assert res2["episode_reward_mean"] > 0, res2
    assert time.time() - start > 2.4
    workers.stop()


def test_rollouts(ray_start_regular_shared):
    workers = make_workers(2)
    a = ParallelRollouts(workers, mode="bulk_sync")
    assert next(a).count == 200
    counters = a.shared_metrics.get().counters
    assert counters["num_steps_sampled"] == 200, metrics
    a = ParallelRollouts(workers, mode="async")
    assert next(a).count == 100
    counters = a.shared_metrics.get().counters
    assert counters["num_steps_sampled"] == 100, metrics
    workers.stop()


def test_rollouts_local(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    assert next(a).count == 100
    counters = a.shared_metrics.get().counters
    assert counters["num_steps_sampled"] == 100, metrics
    workers.stop()


def test_concat_batches(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="async")
    b = a.combine(ConcatBatches(1000))
    assert next(b).count == 1000
    timers = b.shared_metrics.get().timers
    assert "sample" in timers


def test_async_grads(ray_start_regular_shared):
    workers = make_workers(2)
    a = AsyncGradients(workers)
    res1 = next(a)
    assert isinstance(res1, tuple) and len(res1) == 2, res1
    counters = a.shared_metrics.get().counters
    assert counters["num_steps_sampled"] == 100, metrics
    workers.stop()


def test_train_one_step(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(TrainOneStep(workers))
    assert "learner_stats" in next(b)
    counters = a.shared_metrics.get().counters
    assert counters["num_steps_sampled"] == 100, metrics
    assert counters["num_steps_trained"] == 100, metrics
    timers = a.shared_metrics.get().timers
    assert "learn" in timers
    workers.stop()


def test_compute_gradients(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(ComputeGradients(workers))
    grads, counts = next(b)
    assert counts == 100, counts
    assert isinstance(grads, dict), grads
    timers = a.shared_metrics.get().timers
    assert "compute_grads" in timers


def test_avg_gradients(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(ComputeGradients(workers)).batch(4)
    c = b.for_each(AverageGradients)
    grads, counts = next(b)
    assert counts == 400, counts
    assert isinstance(grads, dict), grads


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
