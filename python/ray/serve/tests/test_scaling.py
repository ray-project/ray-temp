import sys

import requests
import pytest

import ray
from ray import serve
from ray.serve.constants import SERVE_PROXY_NAME
from ray.serve.utils import block_until_http_ready
from ray.cluster_utils import Cluster


def test_multiple_routers():
    cluster = Cluster()
    head_node = cluster.add_node()
    cluster.add_node()

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2
    serve.init(http_port=8005)

    # two actors should be started
    head_http = ray.get_actor(SERVE_PROXY_NAME +
                              "-{}-{}".format(node_ids[0], 0))
    ray.get_actor(SERVE_PROXY_NAME + "-{}-{}".format(node_ids[0], 1))

    # wait for the actors to come up
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # kill the head_http server, the HTTP server should still functions
    ray.kill(head_http, no_restart=True)
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
