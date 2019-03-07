import time

import ray
from ray.experimental.serve import RayServeMixin


@ray.remote
class SleepOnFirst(RayServeMixin):
    """Sleep on the first request, return batch size

    Used for testing DeadlineAwareRouter
    """

    def __init__(self, sleep_time):
        self.nap_time = sleep_time

    def __call__(self, input_batch):
        time.sleep(self.nap_time)
        return [len(input_batch) for _ in range(len(input_batch))]


@ray.remote
class SleepCounter(RayServeMixin):
    """Sleep on input argument seconds, return the query id

    Used to test DeadlineAwareRouter
    """

    def __init__(self):
        self.counter = 0

    def __call__(self, input_batch):
        total_sleep_time = sum(input_batch)
        time.sleep(total_sleep_time)

        results = []
        for _ in range(len(input_batch)):
            results.append(self.counter)
            self.counter += 1
        return results
