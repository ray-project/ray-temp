import os

import pytest

import ray
from ray.experimental import serve


@pytest.fixture(scope="session")
def serve_instance():
    if os.path.exists("/tmp/ray_serve.db"):
        os.remove("/tmp/ray_serve.db")
    serve.init(blocking=True)
    yield


@pytest.fixture(scope="session")
def ray_instance():
    ray_already_initialized = ray.is_initialized()
    if not ray_already_initialized:
        ray.init(object_store_memory=int(1e8))
    yield
    if not ray_already_initialized:
        ray.shutdown()
