import asyncio
import csv
from dataclasses import dataclass
import math
import os
from typing import List
import timeit

import numpy as np

import ray


# TODO(Clark): Convert this stats data model to be based on a Pandas DataFrame
# instead of nested data classes.


@dataclass
class StageStats:
    task_durations: List[float]
    stage_duration: float


@dataclass
class MapStats(StageStats):
    read_durations: List[float]


@dataclass
class ReduceStats(StageStats):
    pass


@dataclass
class ConsumeStats:
    consume_times: List[float]


@dataclass
class RoundStats:
    map_stats: MapStats
    reduce_stats: ReduceStats
    consume_stats: ConsumeStats


@dataclass
class MapStatsFromMemory(StageStats):
    pass


@dataclass
class ThrottleStats:
    wait_duration: float


@dataclass
class RoundStatsFromMemory:
    map_stats: MapStatsFromMemory
    reduce_stats: ReduceStats
    consume_stats: ConsumeStats
    throttle_stats: ThrottleStats


@dataclass
class EpochStats:
    round_stats: List[RoundStats]
    duration: float
    throttle_stats: ThrottleStats


@dataclass
class CacheMapStats(StageStats):
    read_durations: List[float]


@dataclass
class EpochStatsFromMemory:
    round_stats: List[RoundStatsFromMemory]
    duration: float
    throttle_stats: ThrottleStats
    cache_map_stats: CacheMapStats


@dataclass
class TrialStats:
    epoch_stats: List[EpochStats]
    duration: float


@dataclass
class TrialStatsFromMemory:
    epoch_stats: List[EpochStatsFromMemory]
    duration: float


# TODO(Clark): Make these actor classes async.


class RoundStatsCollector_:
    def __init__(self, num_maps, num_reduces, num_consumes):
        self._num_maps = num_maps
        self._num_reduces = num_reduces
        self._num_consumes = num_consumes
        self._maps_started = 0
        self._maps_done = 0
        self._map_durations = []
        self._read_durations = []
        self._reduces_started = 0
        self._reduces_done = 0
        self._reduce_durations = []
        self._consume_times = []
        self._consumes_done = 0
        self._throttle_duration = None
        self._map_stage_start_time = None
        self._reduce_stage_start_time = None
        self._map_stage_duration = None
        self._reduce_stage_duration = None

        self._round_done_ev = asyncio.Event()

    def map_start(self):
        if self._maps_started == 0:
            self._map_stage_start(timeit.default_timer())
        self._maps_started += 1

    def map_done(self, duration, read_duration):
        self._maps_done += 1
        self._map_durations.append(duration)
        self._read_durations.append(read_duration)
        if self._maps_done == self._num_maps:
            self._map_stage_done(timeit.default_timer())

    def reduce_start(self):
        if self._reduces_started == 0:
            self._reduce_stage_start(timeit.default_timer())
        self._reduces_started += 1

    def reduce_done(self, duration):
        self._reduces_done += 1
        self._reduce_durations.append(duration)
        if self._reduces_done == self._num_reduces:
            self._reduce_stage_done(timeit.default_timer())

    def consume(self, consume_time):
        self._consumes_done += 1
        self._consume_times.append(consume_time)
        if self._consumes_done == self._num_consumes:
            self._consume_stage_done()

    def throttle_done(self, duration):
        self._throttle_duration = duration

    def _map_stage_start(self, start_time):
        self._map_stage_start_time = start_time

    def _map_stage_done(self, end_time):
        assert self._map_stage_start_time is not None
        self._map_stage_duration = end_time - self._map_stage_start_time

    def _reduce_stage_start(self, start_time):
        self._reduce_stage_start_time = start_time

    def _reduce_stage_done(self, end_time):
        assert self._reduce_stage_start_time is not None
        self._reduce_stage_duration = end_time - self._reduce_stage_start_time

    def _consume_stage_done(self):
        self._round_done_ev.set()

    def get_map_stats(self):
        assert self._map_stage_duration is not None
        assert len(self._map_durations) == self._num_maps
        return MapStats(
            self._map_durations,
            self._map_stage_duration,
            self._read_durations)

    def get_reduce_stats(self):
        assert len(self._reduce_durations) == self._num_reduces
        assert self._reduce_stage_duration is not None
        return ReduceStats(self._reduce_durations, self._reduce_stage_duration)

    def get_consume_stats(self):
        assert len(self._consume_times) == self._num_reduces
        return ConsumeStats(self._consume_times)

    def get_throttle_stats(self):
        if self._throttle_duration is None:
            self._throttle_duration = 0
        return ThrottleStats(self._throttle_duration)

    async def get_stats(self):
        await self._round_done_ev.wait()
        assert self._maps_done == self._num_maps
        assert self._reduces_done == self._num_reduces
        return RoundStats(
            self.get_map_stats(),
            self.get_reduce_stats(),
            self.get_consume_stats(),
            self.get_throttle_stats())


class RoundStatsCollectorFromMemory_(RoundStatsCollector_):
    def map_done(self, duration):
        self._maps_done += 1
        self._map_durations.append(duration)
        if self._maps_done == self._num_maps:
            self._map_stage_done(timeit.default_timer())

    def get_map_stats(self):
        assert self._map_stage_duration is not None
        assert len(self._map_durations) == self._num_maps
        return MapStatsFromMemory(
            self._map_durations, self._map_stage_duration)

    async def get_stats(self):
        await self._round_done_ev.wait()
        assert self._maps_done == self._num_maps
        assert self._reduces_done == self._num_reduces
        return RoundStatsFromMemory(
            self.get_map_stats(),
            self.get_reduce_stats(),
            self.get_consume_stats(),
            self.get_throttle_stats())


class EpochStatsCollector_:
    def __init__(self, num_maps, num_reduces, num_consumes, num_rounds):
        self._collectors = [
            RoundStatsCollector_(num_maps, num_reduces, num_consumes)
            for _ in range(num_rounds)]
        self._duration = None
        self._throttle_duration = None

        self._epoch_done_ev = asyncio.Event()

    def epoch_done(self, duration):
        self._duration = duration
        self._epoch_done_ev.set()

    def throttle_done(self, duration):
        self._throttle_duration = duration

    def round_throttle_done(self, round_idx, duration):
        self._collectors[round_idx].throttle_done(duration)

    def map_start(self, round_idx):
        self._collectors[round_idx].map_start()

    def map_done(self, round_idx, duration, read_duration):
        self._collectors[round_idx].map_done(duration, read_duration)

    def reduce_start(self, round_idx):
        self._collectors[round_idx].reduce_start()

    def reduce_done(self, round_idx, duration):
        self._collectors[round_idx].reduce_done(duration)

    def consume(self, round_idx, consume_time):
        self._collectors[round_idx].consume(consume_time)

    def get_throttle_stats(self):
        if self._throttle_duration is None:
            self._throttle_duration = 0
        return ThrottleStats(self._throttle_duration)

    async def get_stats(self):
        await self._epoch_done_ev.wait()
        round_stats = await asyncio.gather(
            *[
                collector.get_stats()
                for collector in self._collectors])
        assert self._duration is not None
        return EpochStats(
            round_stats,
            self._duration,
            self.get_throttle_stats())


class EpochStatsCollectorFromMemory_(EpochStatsCollector_):
    def __init__(self, num_maps, num_reduces, num_consumes, num_rounds):
        self._collectors = [
            RoundStatsCollectorFromMemory_(
                num_maps, num_reduces, num_consumes)
            for _ in range(num_rounds)]
        self._duration = None
        self._throttle_duration = None
        self._num_cache_maps = num_maps
        self._cache_maps_started = 0
        self._cache_maps_done = 0
        self._cache_map_durations = []
        self._read_durations = []
        self._cache_map_stage_start_time = None
        self._cache_map_stage_duration = None

        self._epoch_done_ev = asyncio.Event()

    def map_done(self, round_idx, duration):
        self._collectors[round_idx].map_done(duration)

    def cache_map_start(self):
        if self._cache_maps_started == 0:
            self._cache_map_stage_start(timeit.default_timer())
        self._cache_maps_started += 1

    def cache_map_done(self, duration, read_duration):
        self._cache_maps_done += 1
        self._cache_map_durations.append(duration)
        self._read_durations.append(read_duration)
        if self._cache_maps_done == self._num_cache_maps:
            self._cache_map_stage_done(timeit.default_timer())

    def _cache_map_stage_start(self, start_time):
        self._cache_map_stage_start_time = start_time

    def _cache_map_stage_done(self, end_time):
        assert self._cache_map_stage_start_time is not None
        self._cache_map_stage_duration = (
            end_time - self._cache_map_stage_start_time)

    def get_cache_map_task_durations(self):
        assert len(self._cache_map_durations) == self._num_cache_maps
        return self._cache_map_durations

    def get_cache_map_stage_duration(self):
        assert self._cache_map_stage_duration is not None
        return self._cache_map_stage_duration

    def get_cache_map_stats(self):
        # TODO(Clark): Yield until this condition is true?
        assert len(self._cache_map_durations) == self._num_cache_maps
        assert self._cache_map_stage_duration is not None
        return CacheMapStats(
            self._cache_map_durations,
            self._cache_map_stage_duration,
            self._read_durations)

    def get_progress(self):
        return self._cache_maps_started, self._cache_maps_done

    async def get_stats(self):
        await self._epoch_done_ev.wait()
        round_stats = await asyncio.gather(
            *[
                collector.get_stats()
                for collector in self._collectors])
        assert self._cache_maps_done == self._num_cache_maps
        assert self._duration is not None
        return EpochStatsFromMemory(
            round_stats,
            self._duration,
            self.get_throttle_stats(),
            self.get_cache_map_stats())


class TrialStatsCollector_:
    def __init__(
            self, num_epochs, num_maps, num_reduces, num_consumes, num_rounds):
        self._collectors = [
            EpochStatsCollector_(
                num_maps, num_reduces, num_consumes, num_rounds)
            for _ in range(num_epochs)]
        self._duration = None

        self._trial_done_ev = asyncio.Event()

    def trial_done(self, duration):
        self._duration = duration
        self._trial_done_ev.set()

    def epoch_throttle_done(self, epoch, duration):
        self._collectors[epoch].throttle_done(duration)

    def round_throttle_done(self, epoch, round_idx, duration):
        self._collectors[epoch].round_throttle_done(round_idx, duration)

    def epoch_done(self, epoch, duration):
        self._collectors[epoch].epoch_done(duration)

    def map_start(self, epoch, round_idx):
        self._collectors[epoch].map_start(round_idx)

    def map_done(self, epoch, round_idx, duration, read_duration):
        self._collectors[epoch].map_done(round_idx, duration, read_duration)

    def reduce_start(self, epoch, round_idx):
        self._collectors[epoch].reduce_start(round_idx)

    def reduce_done(self, epoch, round_idx, duration):
        self._collectors[epoch].reduce_done(round_idx, duration)

    def consume(self, epoch, round_idx, consume_time):
        self._collectors[epoch].consume(round_idx, consume_time)

    async def get_stats(self):
        await self._trial_done_ev.wait()
        epoch_stats = await asyncio.gather(
            *[
                collector.get_stats()
                for collector in self._collectors])
        assert self._duration is not None
        return TrialStats(
            epoch_stats,
            self._duration)


class TrialStatsCollectorFromMemory_(TrialStatsCollector_):
    def __init__(
            self, num_epochs, num_maps, num_reduces, num_consumes, num_rounds):
        self._collectors = [
            EpochStatsCollectorFromMemory_(
                num_maps, num_reduces, num_consumes, num_rounds)
            for _ in range(num_epochs)]
        self._duration = None

        self._trial_done_ev = asyncio.Event()

    def map_done(self, epoch, round_idx, duration):
        self._collectors[epoch].map_done(round_idx, duration)

    def cache_map_start(self, epoch):
        self._collectors[epoch].cache_map_start()

    def cache_map_done(self, epoch, duration, read_duration):
        self._collectors[epoch].cache_map_done(duration, read_duration)

    async def get_stats(self):
        await self._trial_done_ev.wait()
        epoch_stats = await asyncio.gather(
            *[
                collector.get_stats()
                for collector in self._collectors])
        assert self._duration is not None
        return TrialStatsFromMemory(
            epoch_stats,
            self._duration)


TrialStatsCollector = ray.remote(TrialStatsCollector_)
TrialStatsCollectorFromMemory = ray.remote(TrialStatsCollectorFromMemory_)


def process_stats(
        all_stats,
        overwrite_stats,
        stats_dir,
        no_epoch_stats,
        no_round_stats,
        no_consume_stats,
        use_from_disk_shuffler,
        num_row_groups,
        num_rows_per_group,
        num_row_groups_per_file,
        num_rows,
        batch_size,
        batches_per_round,
        num_trainers,
        num_epochs,
        num_rounds,
        max_concurrent_epochs,
        max_concurrent_rounds):
    times = [stats.duration for stats in all_stats]
    mean = np.mean(times)
    std = np.std(times)
    throughput_std = np.std([num_epochs * num_rows / time for time in times])
    batch_throughput_std = np.std([
        (num_epochs * num_rows / batch_size) / time for time in times])
    print(f"\nMean over {len(times)} trials: {mean:.3f}s +- {std}")
    print(f"Mean throughput over {len(times)} trials: "
          f"{num_epochs * num_rows / mean:.2f} rows/s +- {throughput_std:.2f}")
    print(f"Mean batch throughput over {len(times)} trials: "
          f"{(num_epochs * num_rows / batch_size) / mean:.2f} batches/s +- "
          f"{batch_throughput_std:.2f}")

    shuffle_type = (
        "from_disk" if use_from_disk_shuffler else "from_memory")
    overwrite_stats = overwrite_stats
    write_mode = "w+" if overwrite_stats else "a+"
    stats_dir = stats_dir
    hr_num_row_groups = human_readable_big_num(num_row_groups)
    hr_num_rows_per_group = human_readable_big_num(num_rows_per_group)
    hr_batch_size = human_readable_big_num(batch_size)
    filename = (
        f"trial_stats_{shuffle_type}_{hr_num_row_groups}_"
        f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
    filename = os.path.join(stats_dir, filename)
    write_header = (
        overwrite_stats or not os.path.exists(filename) or
        os.path.getsize(filename) == 0)
    print(f"Writing out trial stats to {filename}.")
    # TODO(Clark): Add per-mapper, per-reducer, and per-trainer stat CSVs.

    # TODO(Clark): Add throttling stats to benchmark stats.
    with open(filename, write_mode) as f:
        fieldnames = [
            "shuffle_type",
            "row_groups_per_file",
            "num_trainers",
            "batches_per_round",
            "num_epochs",
            "max_concurrent_epochs",
            "num_rounds",
            "max_concurrent_rounds",
            "trial",
            "duration",
            "row_throughput",
            "batch_throughput",
            "avg_epoch_duration",  # across rounds
            "std_epoch_duration",  # across rounds
            "max_epoch_duration",  # across rounds
            "min_epoch_duration",  # across rounds
            "avg_map_stage_duration",  # across rounds
            "std_map_stage_duration",  # across rounds
            "max_map_stage_duration",  # across rounds
            "min_map_stage_duration",  # across rounds
            "avg_reduce_stage_duration",  # across rounds
            "std_reduce_stage_duration",  # across rounds
            "max_reduce_stage_duration",  # across rounds
            "min_reduce_stage_duration",  # across rounds
            "avg_map_task_duration",  # across rounds and mappers
            "std_map_task_duration",  # across rounds and mappers
            "max_map_task_duration",  # across rounds and mappers
            "min_map_task_duration",  # across rounds and mappers
            "avg_reduce_task_duration",  # across rounds and reducers
            "std_reduce_task_duration",  # across rounds and reducers
            "max_reduce_task_duration",  # across rounds and reducers
            "min_reduce_task_duration",  # across rounds and reducers
            "avg_time_to_consume",  # across rounds and consumers
            "std_time_to_consume",  # across rounds and consumers
            "max_time_to_consume",  # across rounds and consumers
            "min_time_to_consume"]  # across rounds and consumers
        if use_from_disk_shuffler:
            fieldnames += [
                "avg_read_duration",  # across rounds and mappers
                "std_read_duration",  # across rounds and mappers
                "max_read_duration",  # across rounds and mappers
                "min_read_duration"]  # across rounds and mappers
        else:
            fieldnames += [
                "avg_cache_map_stage_duration",  # across mappers
                "std_cache_map_stage_duration",  # across mappers
                "max_cache_map_stage_duration",  # across mappers
                "min_cache_map_stage_duration",  # across mappers
                "avg_cache_map_task_duration",  # across mappers
                "std_cache_map_task_duration",  # across mappers
                "max_cache_map_task_duration",  # across mappers
                "min_cache_map_task_duration",  # across mappers
                "avg_read_duration",  # across rounds
                "std_read_duration",  # across rounds
                "max_read_duration",  # across rounds
                "min_read_duration"]  # across rounds
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {
            "shuffle_type": shuffle_type,
            "row_groups_per_file": num_row_groups_per_file,
            "num_trainers": num_trainers,
            "batches_per_round": batches_per_round,
            "num_epochs": num_epochs,
            "max_concurrent_epochs": max_concurrent_epochs,
            "num_rounds": num_rounds,
            "max_concurrent_rounds": max_concurrent_rounds,
        }
        for trial, stats in enumerate(all_stats):
            row["trial"] = trial
            row["duration"] = stats.duration
            row["row_throughput"] = num_epochs * num_rows / stats.duration
            row["batch_throughput"] = (
                num_epochs * num_rows / batch_size / stats.duration)

            # Get the stats from each epoch and each round.
            epoch_durations = []
            cache_map_stage_durations = []
            cache_map_task_durations = []
            map_task_durations = []
            map_stage_durations = []
            reduce_task_durations = []
            reduce_stage_durations = []
            read_durations = []
            consume_times = []
            for epoch_stats in stats.epoch_stats:
                epoch_durations.append(epoch_stats.duration)
                if not use_from_disk_shuffler:
                    cache_map_stats = epoch_stats.cache_map_stats
                    cache_map_stage_durations.append(
                        cache_map_stats.stage_duration)
                    cache_map_task_durations.extend(
                        cache_map_stats.task_durations)
                    read_durations.extend(cache_map_stats.read_durations)
                for round_stats in epoch_stats.round_stats:
                    map_stats = round_stats.map_stats
                    map_stage_durations.append(map_stats.stage_duration)
                    for duration in map_stats.task_durations:
                        map_task_durations.append(duration)
                    if use_from_disk_shuffler:
                        for duration in map_stats.read_durations:
                            read_durations.append(duration)
                    reduce_stats = round_stats.reduce_stats
                    reduce_stage_durations.append(reduce_stats.stage_duration)
                    for duration in reduce_stats.task_durations:
                        reduce_task_durations.append(duration)
                    for consume_time in (
                            round_stats.consume_stats.consume_times):
                        consume_times.append(consume_time)

            # Calculate the trial stats.
            row["avg_epoch_duration"] = np.mean(epoch_durations)
            row["std_epoch_duration"] = np.std(epoch_durations)
            row["max_epoch_duration"] = np.max(epoch_durations)
            row["min_epoch_duration"] = np.min(epoch_durations)
            row["avg_cache_map_stage_duration"] = np.mean(
                cache_map_stage_durations)
            row["std_cache_map_stage_duration"] = np.std(
                cache_map_stage_durations)
            row["max_cache_map_stage_duration"] = np.max(
                cache_map_stage_durations)
            row["min_cache_map_stage_duration"] = np.min(
                cache_map_stage_durations)
            row["avg_cache_map_task_duration"] = np.mean(
                cache_map_task_durations)
            row["std_cache_map_task_duration"] = np.std(
                cache_map_task_durations)
            row["max_cache_map_task_duration"] = np.max(
                cache_map_task_durations)
            row["min_cache_map_task_duration"] = np.min(
                cache_map_task_durations)
            row["avg_map_stage_duration"] = np.mean(map_stage_durations)
            row["std_map_stage_duration"] = np.std(map_stage_durations)
            row["max_map_stage_duration"] = np.max(map_stage_durations)
            row["min_map_stage_duration"] = np.min(map_stage_durations)
            row["avg_reduce_stage_duration"] = np.mean(reduce_stage_durations)
            row["std_reduce_stage_duration"] = np.std(reduce_stage_durations)
            row["max_reduce_stage_duration"] = np.max(reduce_stage_durations)
            row["min_reduce_stage_duration"] = np.min(reduce_stage_durations)
            row["avg_map_task_duration"] = np.mean(map_task_durations)
            row["std_map_task_duration"] = np.std(map_task_durations)
            row["max_map_task_duration"] = np.max(map_task_durations)
            row["min_map_task_duration"] = np.min(map_task_durations)
            row["avg_reduce_task_duration"] = np.mean(reduce_task_durations)
            row["std_reduce_task_duration"] = np.std(reduce_task_durations)
            row["max_reduce_task_duration"] = np.max(reduce_task_durations)
            row["min_reduce_task_duration"] = np.min(reduce_task_durations)
            row["avg_time_to_consume"] = np.mean(consume_times)
            row["std_time_to_consume"] = np.std(consume_times)
            row["max_time_to_consume"] = np.max(consume_times)
            row["min_time_to_consume"] = np.min(consume_times)
            row["avg_read_duration"] = np.mean(read_durations)
            row["std_read_duration"] = np.std(read_durations)
            row["max_read_duration"] = np.max(read_durations)
            row["min_read_duration"] = np.min(read_durations)
            writer.writerow(row)

    if not no_epoch_stats:
        filename = (
            f"epoch_stats_{shuffle_type}_{hr_num_row_groups}_"
            f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
        filename = os.path.join(stats_dir, filename)
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        print(f"Writing out epoch stats to {filename}.")
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "row_groups_per_file",
                "num_trainers",
                "batches_per_round",
                "num_epochs",
                "max_concurrent_epochs",
                "num_rounds",
                "max_concurrent_rounds",
                "trial",
                "epoch",
                "duration",
                "row_throughput",
                "batch_throughput",
                "avg_map_stage_duration",  # across rounds
                "std_map_stage_duration",  # across rounds
                "max_map_stage_duration",  # across rounds
                "min_map_stage_duration",  # across rounds
                "avg_reduce_stage_duration",  # across rounds
                "std_reduce_stage_duration",  # across rounds
                "max_reduce_stage_duration",  # across rounds
                "min_reduce_stage_duration",  # across rounds
                "avg_map_task_duration",  # across rounds and mappers
                "std_map_task_duration",  # across rounds and mappers
                "max_map_task_duration",  # across rounds and mappers
                "min_map_task_duration",  # across rounds and mappers
                "avg_reduce_task_duration",  # across rounds and reducers
                "std_reduce_task_duration",  # across rounds and reducers
                "max_reduce_task_duration",  # across rounds and reducers
                "min_reduce_task_duration",  # across rounds and reducers
                "avg_time_to_consume",  # across rounds and consumers
                "std_time_to_consume",  # across rounds and consumers
                "max_time_to_consume",  # across rounds and consumers
                "min_time_to_consume"]  # across rounds and consumers
            if use_from_disk_shuffler:
                fieldnames += [
                    "avg_read_duration",  # across rounds and mappers
                    "std_read_duration",  # across rounds and mappers
                    "max_read_duration",  # across rounds and mappers
                    "min_read_duration"]  # across rounds and mappers
            else:
                fieldnames += [
                    "cache_map_stage_duration",
                    "avg_cache_map_task_duration",  # across mappers
                    "std_cache_map_task_duration",  # across mappers
                    "max_cache_map_task_duration",  # across mappers
                    "min_cache_map_task_duration",  # across mappers
                    "avg_read_duration",  # across rounds
                    "std_read_duration",  # across rounds
                    "max_read_duration",  # across rounds
                    "min_read_duration"]  # across rounds
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "row_groups_per_file": num_row_groups_per_file,
                "num_trainers": num_trainers,
                "batches_per_round": batches_per_round,
                "num_epochs": num_epochs,
                "max_concurrent_epochs": max_concurrent_epochs,
                "num_rounds": num_rounds,
                "max_concurrent_rounds": max_concurrent_rounds,
            }
            for trial, trial_stats in enumerate(all_stats):
                row["trial"] = trial
                for epoch, stats in enumerate(trial_stats.epoch_stats):
                    row["epoch"] = epoch
                    row["duration"] = stats.duration
                    row["row_throughput"] = num_rows / stats.duration
                    row["batch_throughput"] = (
                        num_rows / batch_size / stats.duration)

                    # Get the stats from each round.
                    map_task_durations = []
                    map_stage_durations = []
                    reduce_task_durations = []
                    reduce_stage_durations = []
                    read_durations = []
                    consume_times = []
                    for round_stats in stats.round_stats:
                        map_stats = round_stats.map_stats
                        map_stage_durations.append(map_stats.stage_duration)
                        for duration in map_stats.task_durations:
                            map_task_durations.append(duration)
                        if use_from_disk_shuffler:
                            for duration in map_stats.read_durations:
                                read_durations.append(duration)
                        reduce_stats = round_stats.reduce_stats
                        reduce_stage_durations.append(
                            reduce_stats.stage_duration)
                        for duration in reduce_stats.task_durations:
                            reduce_task_durations.append(duration)
                        for consume_time in (
                                round_stats.consume_stats.consume_times):
                            consume_times.append(consume_time)

                    # Calculate the trial stats.
                    row["avg_map_stage_duration"] = np.mean(
                        map_stage_durations)
                    row["std_map_stage_duration"] = np.std(map_stage_durations)
                    row["max_map_stage_duration"] = np.max(map_stage_durations)
                    row["min_map_stage_duration"] = np.min(map_stage_durations)
                    row["avg_reduce_stage_duration"] = np.mean(
                        reduce_stage_durations)
                    row["std_reduce_stage_duration"] = np.std(
                        reduce_stage_durations)
                    row["max_reduce_stage_duration"] = np.max(
                        reduce_stage_durations)
                    row["min_reduce_stage_duration"] = np.min(
                        reduce_stage_durations)
                    row["avg_map_task_duration"] = np.mean(map_task_durations)
                    row["std_map_task_duration"] = np.std(map_task_durations)
                    row["max_map_task_duration"] = np.max(map_task_durations)
                    row["min_map_task_duration"] = np.min(map_task_durations)
                    row["avg_reduce_task_duration"] = np.mean(
                        reduce_task_durations)
                    row["std_reduce_task_duration"] = np.std(
                        reduce_task_durations)
                    row["max_reduce_task_duration"] = np.max(
                        reduce_task_durations)
                    row["min_reduce_task_duration"] = np.min(
                        reduce_task_durations)
                    row["avg_time_to_consume"] = np.mean(consume_times)
                    row["std_time_to_consume"] = np.std(consume_times)
                    row["max_time_to_consume"] = np.max(consume_times)
                    row["min_time_to_consume"] = np.min(consume_times)
                    if not use_from_disk_shuffler:
                        cache_map_stats = stats.cache_map_stats
                        row["cache_map_stage_duration"] = (
                            cache_map_stats.stage_duration)
                        row["avg_cache_map_task_duration"] = np.mean(
                            cache_map_stats.task_durations)
                        row["std_cache_map_task_duration"] = np.std(
                            cache_map_stats.task_durations)
                        row["max_cache_map_task_duration"] = np.max(
                            cache_map_stats.task_durations)
                        row["min_cache_map_task_duration"] = np.min(
                            cache_map_stats.task_durations)
                        for duration in cache_map_stats.read_durations:
                            read_durations.append(duration)
                    row["avg_read_duration"] = np.mean(read_durations)
                    row["std_read_duration"] = np.std(read_durations)
                    row["max_read_duration"] = np.max(read_durations)
                    row["min_read_duration"] = np.min(read_durations)
                    writer.writerow(row)

    if not no_round_stats:
        # TODO(Clark): Add per-round granularity for stats.
        filename = (
            f"round_stats_{shuffle_type}_{hr_num_row_groups}_"
            f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
        filename = os.path.join(stats_dir, filename)
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        print(f"Writing out round stats to {filename}.")
        # TODO(Clark): Add per-mapper, per-reducer, and per-trainer stat CSVs.
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "row_groups_per_file",
                "num_trainers",
                "batches_per_round",
                "num_epochs",
                "max_concurrent_epochs",
                "num_rounds",
                "max_concurrent_rounds",
                "trial",
                "epoch",
                "round",
                "map_stage_duration",
                "reduce_stage_duration",
                "avg_map_task_duration",  # across mappers
                "std_map_task_duration",  # across mappers
                "max_map_task_duration",  # across mappers
                "min_map_task_duration",  # across mappers
                "avg_reduce_task_duration",  # across reducers
                "std_reduce_task_duration",  # across reducers
                "max_reduce_task_duration",  # across reducers
                "min_reduce_task_duration",  # across reducers
                "avg_time_to_consume",  # across consumers
                "std_time_to_consume",  # across consumers
                "max_time_to_consume",  # across consumers
                "min_time_to_consume"]  # across consumers
            if use_from_disk_shuffler:
                fieldnames += [
                    "avg_read_duration",  # across mappers
                    "std_read_duration",  # across mappers
                    "max_read_duration",  # across mappers
                    "min_read_duration"]  # across mappers
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "row_groups_per_file": num_row_groups_per_file,
                "num_trainers": num_trainers,
                "batches_per_round": batches_per_round,
                "num_epochs": num_epochs,
                "max_concurrent_epochs": max_concurrent_epochs,
                "num_rounds": num_rounds,
                "max_concurrent_rounds": max_concurrent_rounds,
            }
            for trial, trial_stats in enumerate(all_stats):
                row["trial"] = trial
                for epoch, epoch_stats in enumerate(trial_stats.epoch_stats):
                    row["epoch"] = epoch
                    for round_idx, stats in enumerate(epoch_stats.round_stats):
                        row["round"] = round_idx
                        row["map_stage_duration"] = (
                            stats.map_stats.stage_duration)
                        row["reduce_stage_duration"] = (
                            stats.reduce_stats.stage_duration)
                        row["avg_map_task_duration"] = np.mean(
                            stats.map_stats.task_durations)
                        row["std_map_task_duration"] = np.std(
                            stats.map_stats.task_durations)
                        row["max_map_task_duration"] = np.max(
                            stats.map_stats.task_durations)
                        row["min_map_task_duration"] = np.min(
                            stats.map_stats.task_durations)
                        row["avg_reduce_task_duration"] = np.mean(
                            stats.reduce_stats.task_durations)
                        row["std_reduce_task_duration"] = np.std(
                            stats.reduce_stats.task_durations)
                        row["max_reduce_task_duration"] = np.max(
                            stats.reduce_stats.task_durations)
                        row["min_reduce_task_duration"] = np.min(
                            stats.reduce_stats.task_durations)
                        row["avg_time_to_consume"] = np.mean(
                            stats.consume_stats.consume_times)
                        row["std_time_to_consume"] = np.std(
                            stats.consume_stats.consume_times)
                        row["max_time_to_consume"] = np.max(
                            stats.consume_stats.consume_times)
                        row["min_time_to_consume"] = np.min(
                            stats.consume_stats.consume_times)
                        if use_from_disk_shuffler:
                            row["avg_read_duration"] = np.mean(
                                stats.map_stats.read_durations)
                            row["std_read_duration"] = np.std(
                                stats.map_stats.read_durations)
                            row["max_read_duration"] = np.max(
                                stats.map_stats.read_durations)
                            row["min_read_duration"] = np.min(
                                stats.map_stats.read_durations)
                        writer.writerow(row)
    if not no_consume_stats:
        # TODO(Clark): Add per-round granularity for stats.
        filename = (
            f"consume_stats_{shuffle_type}_{hr_num_row_groups}_"
            f"{hr_num_rows_per_group}_{hr_batch_size}.csv")
        filename = os.path.join(stats_dir, filename)
        print(f"Writing out consume stats to {filename}.")
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "row_groups_per_file",
                "num_trainers",
                "batches_per_round",
                "num_epochs",
                "max_concurrent_epochs",
                "num_rounds",
                "max_concurrent_rounds",
                "trial",
                "epoch",
                "round",
                "consumer",
                "consume_time"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "row_groups_per_file": num_row_groups_per_file,
                "num_trainers": num_trainers,
                "batches_per_round": batches_per_round,
                "num_epochs": num_epochs,
                "max_concurrent_epochs": max_concurrent_epochs,
                "num_rounds": num_rounds,
                "max_concurrent_rounds": max_concurrent_rounds,
            }
            for trial, trial_stats in enumerate(all_stats):
                row["trial"] = trial
                for epoch, epoch_stats in enumerate(trial_stats.epoch_stats):
                    row["epoch"] = epoch
                    for round_idx, stats in enumerate(epoch_stats.round_stats):
                        row["round"] = round_idx
                        for consumer, consume_time in enumerate(
                                stats.consume_stats.consume_times):
                            # NOTE: Consumer identifiers are not consistent
                            # across rounds.
                            row["consumer"] = consumer
                            row["consume_time"] = consume_time
                            writer.writerow(row)


UNITS = ["", "K", "M", "B", "T", "Q"]


def human_readable_big_num(num):
    idx = int(math.log10(num) // 3)
    unit = UNITS[idx]
    new_num = num / 10 ** (3 * idx)
    if new_num % 1 == 0:
        return f"{int(new_num)}{unit}"
    else:
        return f"{new_num:.1f}{unit}"
