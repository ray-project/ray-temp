import numpy as np
import unittest

import ray

MB = 1024 * 1024

OBJECT_EVICTED = ray.exceptions.UnreconstructableError
OBJECT_TOO_LARGE = ray.exceptions.ObjectStoreFullError


@ray.remote
class LightActor(object):
    def __init__(self):
        pass

    def sample(self):
        return np.zeros(5 * MB, dtype=np.uint8)


@ray.remote
class GreedyActor(object):
    def __init__(self):
        pass

    def sample(self):
        return np.zeros(20 * MB, dtype=np.uint8)


class TestMemoryLimits(unittest.TestCase):
    def testWithoutQuota(self):
        self.assertRaises(OBJECT_EVICTED,
                          lambda: self._run(100 * MB, None, None))

    def _run(self, driver_quota, a_quota, b_quota):
        print("*** Testing ***", driver_quota, a_quota, b_quota)
        try:
            ray.init(
                num_cpus=1,
                object_store_memory=300 * MB,
                driver_object_store_memory=driver_quota)
            z = ray.put("hi", weakref=True)
            a = LightActor._remote(object_store_memory=a_quota)
            b = GreedyActor._remote(object_store_memory=b_quota)
            for _ in range(5):
                r_a = a.sample.remote()
                for _ in range(20):
                    new_oid = b.sample.remote()
                    ray.get(new_oid)
                ray.get(r_a)
            ray.get(z)
        except Exception as e:
            print("Raised exception", type(e), e)
            raise e
        finally:
            print(ray.worker.global_worker.core_worker.
                  dump_object_store_memory_usage())
            ray.shutdown()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
