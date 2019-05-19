"""
This file defines the common pytest fixtures used in current directory.
"""

from contextlib import contextmanager
import json
import pytest
import subprocess

import ray
from ray.tests.cluster_utils import Cluster
from ray.tests.utils import run_and_get_output


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def generate_internal_config_map(**kwargs):
    internal_config = json.dumps(kwargs)
    ray_kwargs = {
        "_internal_config": internal_config,
    }
    return ray_kwargs


def get_default_fixure_internal_config():
    internal_config = json.dumps({
        "initial_reconstruction_timeout_milliseconds": 200,
        "num_heartbeats_timeout": 10,
    })
    return internal_config


def get_default_fixture_ray_kwargs():
    internal_config = get_default_fixure_internal_config()
    ray_kwargs = {
        "num_cpus": 1,
        "object_store_memory": 10**8,
        "_internal_config": internal_config,
    }
    return ray_kwargs


@contextmanager
def _ray_start(**kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    init_kwargs.update(kwargs)
    # Start the Ray processes.
    address_info = ray.init(**init_kwargs)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


# The following fixture will start ray with 1 cpu.
@pytest.fixture
def ray_start_regular(request):
    param = getattr(request, "param", {})
    with _ray_start(**param) as res:
        yield res


@pytest.fixture
def ray_start_2_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
        yield res


@pytest.fixture
def ray_start_10_cpus(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=10, **param) as res:
        yield res


@contextmanager
def _ray_start_cluster(num_nodes=0, do_init=False, **kwargs):
    init_kwargs = get_default_fixture_ray_kwargs()
    if num_nodes > 0:
        do_init = True
    init_kwargs.update(kwargs)
    cluster = Cluster()
    remote_nodes = []
    for i in range(num_nodes):
        remote_nodes.append(cluster.add_node(**init_kwargs))
        # Make sure the driver is conencting to the head node.
    if do_init:
        ray.init(redis_address=cluster.redis_address)
    yield cluster
    # The code after the yield will run as teardown code.
    ray.shutdown()
    cluster.shutdown()


# This fixture will start a cluster with empty nodes.
@pytest.fixture
def ray_start_cluster(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(**param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_head(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(num_nodes=1, do_init=True, **param) as res:
        yield res


@pytest.fixture
def ray_start_cluster_2_nodes(request):
    param = getattr(request, "param", {})
    with _ray_start_cluster(num_nodes=2, do_init=True, **param) as res:
        yield res


@pytest.fixture
def ray_start_object_store_memory(request):
    # Start the Ray processes.
    store_size = request.param
    internal_config = get_default_fixure_internal_config()
    init_kwargs = {
        "num_cpus": 1,
        "_internal_config": internal_config,
        "object_store_memory": store_size,
    }
    ray.init(**init_kwargs)
    yield store_size
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def call_ray_start(request):
    parameter = getattr(request, "param", "ray start --head --num-cpus=1")
    command_args = parameter.split(" ")
    out = run_and_get_output(command_args)
    # Get the redis address from the output.
    redis_substring_prefix = "redis_address=\""
    redis_address_location = (
        out.find(redis_substring_prefix) + len(redis_substring_prefix))
    redis_address = out[redis_address_location:]
    redis_address = redis_address.split("\"")[0]

    yield redis_address

    # Disconnect from the Ray cluster.
    ray.shutdown()
    # Kill the Ray cluster.
    subprocess.Popen(["ray", "stop"]).wait()
