import os

import ray
from ray import tune
from ray.tune.cluster_info import is_ray_cluster

from _trainable import timed_tune_run


def main():
    os.environ["TUNE_DISABLE_AUTO_CALLBACK_LOGGERS"] = "1"  # Tweak

    ray.init(address="auto")

    num_samples = 1000
    results_per_second = 10
    trial_length_s = 100

    max_runtime = 120

    if is_ray_cluster():
        # Add constant overhead for SSH connection
        max_runtime = 120

    timed_tune_run(
        name="result throughput cluster",
        num_samples=num_samples,
        results_per_second=results_per_second,
        trial_length_s=trial_length_s,
        max_runtime=max_runtime,
        sync_config=tune.SyncConfig(sync_to_driver=False))  # Tweak!


if __name__ == "__main__":
    main()
