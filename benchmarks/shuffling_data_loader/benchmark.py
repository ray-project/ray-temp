import argparse
import glob
import os
import timeit

import numpy as np

import ray
from ray.experimental.data_loader.shuffle import (
    shuffle_from_disk, shuffle_from_memory_with_stats,
    shuffle_from_memory_no_stats)
from ray.experimental.data_loader.stats import (process_stats,
                                                human_readable_size)

from ray.experimental.data_loader.data_generation import generate_data

# TODOs:
# - [DONE] Add support for multiple epochs in a single trial.
# - [DONE] Add task graph for from memory shuffler to external doc.
# - [DONE] Create throughput stability graph for 3 or 4 of the best round
#   configurations, including both from memory and from disk shuffler results.
# - [DONE] Show that number of trainers doesn't degrade throughput.
# - [DONE] Explain 40+ second overhead and that training runs need to be at
#   least as long as that.
# - [DONE] Note what memory footprint should be, and that it's dependent on how
#   aggressively rounds and epochs are pipelined.
# - [DONE] Think about backpressure: calculate how many rounds we can do
#   concurrently, apply application-level backpressure with ray.wait().
# - [DONE] Conclusions attached to those graphs.
# - Explore streaming implementation of cache map stage, where we sample and
#   pop one round partition at a time.

# TODOs:
# - [DONE] Instrument profiling:
#   - Get some basic metrics: disk read time, shuffle time between map and
#     reduce tasks, average map/reduce task duration.
# - [DONE] Plot the results.
# - [DONE] Compute number of rounds based on batch size.
# - [DONE] Run on a large machine with the full dataset size
# - Scale past memory capacity of the cluster (long-term)

# Dataset info:
#
# 100 row groups
# 4M rows/group
#  - Size of row group can vary from 30k to 4M
# ~52GB total, on disk, snappy compressed
# 256k - 512k rows/batch
#  - batch size could be as small as 50k
#  - has target in bytes
#  - is arrived at iteratively, can vary across models
# 4M rows/group, 256k rows/batch -> 170MB/file

DEFAULT_DATA_DIR = "/mnt/disk0/benchmark_scratch"
DEFAULT_STATS_DIR = "./results"

DEFAULT_UTILIZATION_SAMPLE_PERIOD = 5.0


def dummy_batch_consumer(consumer_idx, epoch, batches):
    pass


def run_trials(num_epochs,
               filenames,
               num_reducers,
               num_trainers,
               max_concurrent_epochs,
               utilization_sample_period,
               collect_stats=True,
               use_from_disk_shuffler=False,
               num_trials=None,
               trials_timeout=None):
    if use_from_disk_shuffler:
        print("Using from-disk shuffler that loads data from disk each round.")
        shuffle = shuffle_from_disk
    else:
        print("Using from-memory shuffler.")
        if collect_stats:
            shuffle = shuffle_from_memory_with_stats
        else:
            shuffle = shuffle_from_memory_no_stats
    all_stats = []
    if num_trials is not None:
        for trial in range(num_trials):
            print(f"Starting trial {trial}.")
            stats, store_stats = shuffle(
                filenames, dummy_batch_consumer, num_epochs, num_reducers,
                num_trainers, max_concurrent_epochs, utilization_sample_period)
            duration = stats.duration if collect_stats else stats
            print(f"Trial {trial} done after {duration} seconds.")
            all_stats.append((stats, store_stats))
    elif trials_timeout is not None:
        start = timeit.default_timer()
        trial = 0
        while timeit.default_timer() - start < trials_timeout:
            print(f"Starting trial {trial}.")
            stats, store_stats = shuffle(
                filenames, dummy_batch_consumer, num_epochs, num_reducers,
                num_trainers, max_concurrent_epochs, utilization_sample_period)
            duration = stats.duration if collect_stats else stats
            print(f"Trial {trial} done after {duration} seconds.")
            all_stats.append((stats, store_stats))
            trial += 1
    else:
        raise ValueError(
            "One of num_trials and trials_timeout must be specified")
    return all_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shuffling data loader")
    parser.add_argument("--num-rows", type=int, default=4 * (10**11))
    parser.add_argument("--num-files", type=int, default=100)
    parser.add_argument("--max-row-group-skew", type=float, default=0.0)
    parser.add_argument("--num-row-groups-per-file", type=int, default=1)
    parser.add_argument("--num-reducers", type=int, default=5)
    parser.add_argument("--num-trainers", type=int, default=5)
    parser.add_argument("--num-epochs", type=int, default=10)
    parser.add_argument("--max-concurrent-epochs", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--num-trials", type=int, default=None)
    parser.add_argument("--trials-timeout", type=int, default=None)
    parser.add_argument(
        "--utilization-sample-period",
        type=float,
        default=DEFAULT_UTILIZATION_SAMPLE_PERIOD)
    parser.add_argument("--use-from-disk-shuffler", action="store_true")
    parser.add_argument("--cluster", action="store_true")
    parser.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR)
    parser.add_argument("--stats-dir", type=str, default=DEFAULT_STATS_DIR)
    parser.add_argument("--clear-old-data", action="store_true")
    parser.add_argument("--use-old-data", action="store_true")
    parser.add_argument("--no-stats", action="store_true")
    parser.add_argument("--no-epoch-stats", action="store_true")
    parser.add_argument("--no-consume-stats", action="store_true")
    parser.add_argument("--overwrite-stats", action="store_true")
    args = parser.parse_args()

    if args.num_row_groups_per_file < 1:
        raise ValueError("Must have at least one row group per file.")

    num_trials = args.num_trials
    trials_timeout = args.trials_timeout
    if num_trials is not None and trials_timeout is not None:
        raise ValueError(
            "Only one of --num-trials and --trials-timeout should be "
            "specified.")

    if num_trials is None and trials_timeout is None:
        num_trials = 3

    if args.clear_old_data and args.use_old_data:
        raise ValueError(
            "Only one of --clear-old-data and --use-old-data should be "
            "specified.")

    data_dir = args.data_dir
    if args.clear_old_data:
        print(f"Clearing old data from {data_dir}.")
        files = glob.glob(os.path.join(data_dir, "*.parquet.snappy"))
        for f in files:
            os.remove(f)

    if args.cluster:
        print("Connecting to an existing Ray cluster.")
        ray.init(address="auto")
    else:
        print("Starting a new local Ray cluster.")
        ray.init()

    num_rows = args.num_rows
    num_row_groups_per_file = args.num_row_groups_per_file
    num_files = args.num_files
    max_row_group_skew = args.max_row_group_skew
    if not args.use_old_data:
        print(f"Generating {num_rows} rows over {num_files} files, with "
              f"{num_row_groups_per_file} row groups per file and at most "
              f"{100 * max_row_group_skew:.1f}% row group skew.")
        filenames, num_bytes = generate_data(num_rows, num_files,
                                             num_row_groups_per_file,
                                             max_row_group_skew, data_dir)
        print(f"Generated {len(filenames)} files containing {num_rows} rows "
              f"with {num_row_groups_per_file} row groups per file, totalling "
              f"{human_readable_size(num_bytes)}.")
    else:
        filenames = [
            os.path.join(data_dir, f"input_data_{file_index}.parquet.snappy")
            for file_index in range(num_files)
        ]
        print("Not generating input data, using existing data instead.")

    num_reducers = args.num_reducers
    num_trainers = args.num_trainers
    batch_size = args.batch_size

    num_epochs = args.num_epochs
    max_concurrent_epochs = args.max_concurrent_epochs
    if max_concurrent_epochs is None or max_concurrent_epochs > num_epochs:
        max_concurrent_epochs = num_epochs
    assert max_concurrent_epochs > 0

    utilization_sample_period = args.utilization_sample_period

    # warmup_trials = 2
    # print(f"\nRunning {warmup_trials} warmup trials.")
    # times = run_trials(
    #     filenames,
    #     num_trainers,
    #     num_rows,
    #     warmup_trials)

    print("\nRunning real trials.")
    use_from_disk_shuffler = args.use_from_disk_shuffler
    # TODO(Clark): Reenable from-disk shuffler? Or delete it.
    if use_from_disk_shuffler:
        raise NotImplementedError(
            "From disk shuffler not yet updated for new config.")
    if num_trials is not None:
        print(f"Running {num_trials} shuffle trials with {num_epochs} epochs, "
              f"{num_reducers} reducers, {num_trainers} trainers, and a batch "
              f"size of {batch_size} over {num_rows} rows.")
    else:
        print(f"Running {trials_timeout} seconds of shuffle trials with "
              f"{num_epochs} epochs, {num_reducers} reducers, {num_trainers} "
              f"trainers, and a batch size of {batch_size} over {num_rows} "
              "rows.")
    print(f"Shuffling will be pipelined with at most "
          f"{max_concurrent_epochs} concurrent epochs.")
    collect_stats = not args.no_stats
    all_stats = run_trials(num_epochs, filenames, num_reducers, num_trainers,
                           max_concurrent_epochs, utilization_sample_period,
                           collect_stats, use_from_disk_shuffler, num_trials,
                           trials_timeout)

    if collect_stats:
        process_stats(all_stats, args.overwrite_stats, args.stats_dir,
                      args.no_epoch_stats, args.no_consume_stats,
                      use_from_disk_shuffler, num_rows,
                      num_row_groups_per_file, batch_size, num_reducers,
                      num_trainers, num_epochs, max_concurrent_epochs)
    else:
        print("Shuffle trials done, no detailed stats collected.")
        times, _ = zip(*all_stats)
        mean = np.mean(times)
        std = np.std(times)
        throughput_std = np.std(
            [num_epochs * num_rows / time for time in times])
        batch_throughput_std = np.std(
            [(num_epochs * num_rows / batch_size) / time for time in times])
        print(f"\nMean over {len(times)} trials: {mean:.3f}s +- {std}")
        print(f"Mean throughput over {len(times)} trials: "
              f"{num_epochs * num_rows / mean:.2f} rows/s +- "
              f"{throughput_std:.2f}")
        print(f"Mean batch throughput over {len(times)} trials: "
              f"{(num_epochs * num_rows / batch_size) / mean:.2f} batches/s "
              f"+- {batch_throughput_std:.2f}")