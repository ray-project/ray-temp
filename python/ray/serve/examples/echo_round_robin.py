"""
Example showing round robin policy. The outputs from
v1 and v2 will be (almost) interleaved as queries get processed.
"""
import time

import requests

from ray import serve
from ray.serve.utils import pformat_color_json


def echo_v1(_):
    return "v1"


def echo_v2(_):
    return "v2"


# specify the router policy as RoundRobin
serve.init(queueing_policy=serve.RoutePolicy.RoundRobin)

# create first backend
serve.create_backend("echo:v1", echo_v1)

# create a service backend by the first backend
serve.create_endpoint("my_endpoint", backend="echo:v1", route="/echo")

# create second backend
serve.create_backend("echo:v2", echo_v2)

# split the service between the two backends
serve.set_traffic("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
