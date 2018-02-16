from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import queue
import random
import time
import threading

import ray
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.timer import TimerStat

REPLAY_QUEUE_SIZE = 4
LEARNER_QUEUE_SIZE = 4


class TaskPool(object):
    def __init__(self):
        self._tasks = {}
        self._completed = []

    def add(self, worker, obj_id):
        self._tasks[obj_id] = worker

    def completed(self):
        pending = list(self._tasks)
        if pending:
            ready, _ = ray.wait(pending, num_returns=len(pending), timeout=10)
            for obj_id in ready:
                yield (self._tasks.pop(obj_id), obj_id)

    def take_one(self):
        if not self._completed:
            pending = list(self._tasks)
            for worker, obj_id in self.completed():
                self._completed.append((worker, obj_id))
        if self._completed:
            return self._completed.pop(0)
        else:
            [obj_id], _ = ray.wait(pending, num_returns=1)
            return (self._tasks.pop(obj_id), obj_id)

    @property
    def count(self):
        return len(self._tasks)


@ray.remote
class ReplayActor(object):
    def __init__(self, config, num_shards):
        self.config = config
        self.replay_starts = self.config["learning_starts"] // num_shards
        self.buffer_size = self.config["buffer_size"] // num_shards
        if self.config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                self.buffer_size,
                alpha=self.config["prioritized_replay_alpha"])
        else:
            self.replay_buffer = ReplayBuffer(self.buffer_size)

    def add_batch(self, batch):
        for row in batch.rows():
            self.replay_buffer.add(
                row["obs"], row["actions"], row["rewards"], row["new_obs"],
                row["dones"], row["weights"])

    def replay(self):
        if len(self.replay_buffer) < self.replay_starts:
            return None

        if self.config["prioritized_replay"]:
            (obses_t, actions, rewards, obses_tp1,
                dones, weights, batch_indexes) = self.replay_buffer.sample(
                    self.config["train_batch_size"],
                    beta=self.config["prioritized_replay_beta"])
        else:
            (obses_t, actions, rewards, obses_tp1,
                dones) = self.replay_buffer.sample(
                    self.config["train_batch_size"])
            weights = np.ones_like(rewards)
            batch_indexes = - np.ones_like(rewards)

        return SampleBatch({
            "obs": obses_t, "actions": actions, "rewards": rewards,
            "new_obs": obses_tp1, "dones": dones, "weights": weights,
            "batch_indexes": batch_indexes})

    def update_priorities(self, batch, td_errors):
        if self.config["prioritized_replay"]:
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(
                batch["batch_indexes"], new_priorities)

    def stats(self):
        return self.replay_buffer.stats()


class Learner(threading.Thread):
    def __init__(self, local_evaluator):
        threading.Thread.__init__(self)
        self.local_evaluator = local_evaluator
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_SIZE)
        self.outqueue = queue.Queue()
        self.daemon = True

    def run(self):
        while True:
            ra, replay = self.inqueue.get()
            td_error = self.local_evaluator.compute_apply(replay)
            if td_error is not None:
                self.outqueue.put((ra, replay, td_error))


class ApexOptimizer(Optimizer):

    def _init(self):
        self.learner = Learner(self.local_evaluator)
        self.learner.start()

        num_replay_actors = self.config["num_replay_buffer_shards"]
        self.replay_actors = [
            ReplayActor.remote(self.config, num_replay_actors)
            for _ in range(num_replay_actors)]
        assert len(self.remote_evaluators) > 0

        # Stats
        self.put_weights_timer = TimerStat()
        self.sample_processing = TimerStat()
        self.replay_processing_timer = TimerStat()
        self.train_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.num_weight_syncs = 0
        self.num_samples_added = 0
        self.num_samples_trained = 0

        # Number of worker steps since the last weight update
        self.steps_since_update = {}

        # Otherwise kick of replay tasks for local gradient updates
        self.replay_tasks = TaskPool()
        for ra in self.replay_actors:
            for _ in range(REPLAY_QUEUE_SIZE):
                self.replay_tasks.add(ra, ra.replay.remote())

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            self.steps_since_update[ev] = 0
            self.sample_tasks.add(ev, ev.sample.remote())

    def step(self):
        start = time.time()
        sample_timesteps, train_timesteps = self._step()
        time_delta = time.time() - start
        self.sample_timer.push(time_delta)
        self.sample_timer.push_units_processed(sample_timesteps)
        if train_timesteps > 0:
            self.train_timer.push(time_delta)
            self.train_timer.push_units_processed(train_timesteps)
        self.num_samples_added += sample_timesteps
        self.num_samples_trained += train_timesteps
        return sample_timesteps

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        with self.put_weights_timer:
            weights = ray.put(self.local_evaluator.get_weights())

        with self.sample_processing:
            for ev, sample_batch in self.sample_tasks.completed():
                sample_timesteps += self.config["sample_batch_size"]

                # Send the data to the replay buffer
                random.choice(self.replay_actors).add_batch.remote(
                    sample_batch)

                # Update weights if needed
                self.steps_since_update[ev] += self.config["sample_batch_size"]
                if (self.steps_since_update[ev] >=
                        self.config["max_weight_sync_delay"]):
                    ev.set_weights.remote(weights)
                    self.num_weight_syncs += 1
                    self.steps_since_update[ev] = 0

                # Kick off another sample request
                self.sample_tasks.add(ev, ev.sample.remote())

        with self.replay_processing_timer:
            for ra, replay in self.replay_tasks.completed():
                self.replay_tasks.add(ra, ra.replay.remote())
                self.learner.inqueue.put((ra, ray.get(replay)))
            while not self.learner.outqueue.empty():
                ra, replay, td_error = self.learner.outqueue.get()
                ra.update_priorities.remote(replay, td_error)
                train_timesteps += self.config["train_batch_size"]

        return sample_timesteps, train_timesteps

    def stats(self):
        replay_stats = ray.get(self.replay_actors[0].stats.remote())
        return {
            "replay_shard_0": replay_stats,
            "timing_breakdown": {
                "0_put_weights_time_ms": round(
                    1000 * self.put_weights_timer.mean, 3),
                "1_sample_processing_time_ms": round(
                    1000 * self.sample_processing.mean, 3),
                "2_replay_processing_time_ms": round(
                    1000 * self.replay_processing_timer.mean, 3),
            },
            "sample_throughput": round(self.sample_timer.mean_throughput, 3),
            "train_throughput": round(self.train_timer.mean_throughput, 3),
            "num_weight_syncs": self.num_weight_syncs,
            "num_samples_trained": self.num_samples_trained,
            "pending_sample_tasks": self.sample_tasks.count,
            "pending_replay_tasks": self.replay_tasks.count,
            "learner_queue_size": self.learner.inqueue.qsize(),
        }
