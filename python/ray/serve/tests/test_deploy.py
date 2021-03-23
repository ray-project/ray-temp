from collections import defaultdict
import os
import time

import pytest
import requests

import ray
from ray.test_utils import SignalActor
from ray import serve
from ray.serve.utils import get_random_letters


@pytest.mark.parametrize("use_handle", [True, False])
def test_deploy(serve_instance, use_handle):
    client = serve_instance

    name = "test"

    def call():
        if use_handle:
            ret = ray.get(client.get_handle(name).remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    def v1(*args):
        return f"1|{os.getpid()}"

    def v2(*args):
        return f"2|{os.getpid()}"

    client.deploy(name, v1, version="1")
    val1, pid1 = call()
    assert val1 == "1"

    # Redeploying with the same version and code should do nothing.
    client.deploy(name, v1, version="1")
    val2, pid2 = call()
    assert val2 == "1"
    assert pid2 == pid1

    # Redeploying with a new version should start a new actor.
    client.deploy(name, v1, version="2")
    val3, pid3 = call()
    assert val3 == "1"
    assert pid3 != pid2

    # Redeploying with the same version and new code should do nothing.
    client.deploy(name, v2, version="2")
    val4, pid4 = call()
    assert val4 == "1"
    assert pid4 == pid3

    # Redeploying with new code and a new version should start a new actor
    # running the new code.
    client.deploy(name, v2, version="3")
    val5, pid5 = call()
    assert val5 == "2"
    assert pid5 != pid4


@pytest.mark.parametrize("use_handle", [True, False])
def test_config_change(serve_instance, use_handle):
    client = serve_instance

    name = "test"

    def call():
        if use_handle:
            ret = ray.get(client.get_handle(name).remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    class Backend:
        def __init__(self):
            self.ret = "1"

        def reconfigure(self, d):
            self.ret = d["ret"]

        def __call__(self, *args):
            return f"{self.ret}|{os.getpid()}"

    # First deploy with no user config set.
    client.deploy(name, Backend, version="1")
    val1, pid1 = call()
    assert val1 == "1"

    # Now update the user config without changing versions. Actor should stay
    # alive but return value should change.
    client.deploy(
        name, Backend, version="1", config={"user_config": {
            "ret": "2"
        }})
    val2, pid2 = call()
    assert pid2 == pid1
    assert val2 == "2"

    # Update the user config without changing the version again.
    client.deploy(
        name, Backend, version="1", config={"user_config": {
            "ret": "3"
        }})
    val3, pid3 = call()
    assert pid3 == pid2
    assert val3 == "3"

    # Update the version without changing the user config.
    client.deploy(
        name, Backend, version="2", config={"user_config": {
            "ret": "3"
        }})
    val4, pid4 = call()
    assert pid4 != pid3
    assert val4 == "3"

    # Update the version and the user config.
    client.deploy(
        name, Backend, version="3", config={"user_config": {
            "ret": "4"
        }})
    val5, pid5 = call()
    assert pid5 != pid4
    assert val5 == "4"


@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_single_replica(serve_instance, use_handle):
    # Tests that redeploying a deployment with a single replica waits for the
    # replica to completely shut down before starting a new one.
    client = serve_instance

    name = "test"

    @ray.remote
    def call(block=False):
        if use_handle:
            ret = ray.get(serve.get_handle(name).remote(block=str(block)))
        else:
            ret = requests.get(
                f"http://localhost:8000/{name}", params={
                    "block": block
                }).text

        return ret.split("|")[0], ret.split("|")[1]

    signal_name = f"signal-{get_random_letters()}"
    signal = SignalActor.options(name=signal_name).remote()

    async def v1(request):
        if request.query_params["block"] == "True":
            signal = ray.get_actor(signal_name)
            await signal.wait.remote()
        return f"1|{os.getpid()}"

    def v2(*args):
        return f"2|{os.getpid()}"

    client.deploy(name, v1, version="1")
    ref1 = call.remote(block=False)
    val1, pid1 = ray.get(ref1)
    assert val1 == "1"

    # ref2 will block until the signal is sent.
    ref2 = call.remote(block=True)
    assert len(ray.wait([ref2], timeout=0.1)[0]) == 0

    # Redeploy new version. This should not go through until the old version
    # replica completely stops.
    goal_ref = client.deploy(name, v2, version="2", _blocking=False)
    assert not client._wait_for_goal(goal_ref, timeout=0.1)

    # It may take some time for the handle change to propagate and requests
    # to get sent to the new version. Repeatedly send requests until they
    # start blocking
    start = time.time()
    new_version_ref = None
    while time.time() - start < 30:
        ready, not_ready = ray.wait([call.remote(block=False)], timeout=0.5)
        if len(ready) == 1:
            # If the request doesn't block, it must have been the old version.
            val, pid = ray.get(ready[0])
            assert val == "1"
            assert pid == pid1
        elif len(not_ready) == 1:
            # If the request blocks, it must have been the new version.
            new_version_ref = not_ready[0]
            break
    else:
        assert False, "Timed out waiting for new version to be called."

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val2, pid2 = ray.get(ref2)
    assert val2 == "1"
    assert pid2 == pid1

    # Now the goal and request to the new version should complete.
    assert client._wait_for_goal(goal_ref)
    new_version_val, new_version_pid = ray.get(new_version_ref)
    assert new_version_val == "2"
    assert new_version_pid != pid2


@pytest.mark.parametrize("use_handle", [True, False])
def test_redeploy_multiple_replicas(serve_instance, use_handle):
    # Tests that redeploying a deployment with multiple replicas performs
    # a rolling update.
    client = serve_instance

    name = "test"

    @ray.remote
    def call(block=False):
        if use_handle:
            handle = serve.get_handle(name, missing_ok=True)
            ret = ray.get(handle.remote(block=str(block)))
        else:
            ret = requests.get(
                f"http://localhost:8000/{name}", params={
                    "block": block
                }).text

        return ret.split("|")[0], ret.split("|")[1]

    signal_name = f"signal-{get_random_letters()}"
    signal = SignalActor.options(name=signal_name).remote()

    async def v1(request):
        if request.query_params["block"] == "True":
            signal = ray.get_actor(signal_name)
            await signal.wait.remote()
        return f"1|{os.getpid()}"

    def v2(*args):
        return f"2|{os.getpid()}"

    def make_nonblocking_calls(expected, expect_blocking=False):
        # Returns dict[val, set(pid)].
        blocking = []
        responses = defaultdict(set)
        start = time.time()
        while time.time() - start < 30:
            refs = [call.remote(block=False) for _ in range(10)]
            ready, not_ready = ray.wait(refs, timeout=0.1)
            for ref in ready:
                val, pid = ray.get(ref)
                responses[val].add(pid)
            for ref in not_ready:
                blocking.extend(not_ready)

            if (all(
                    len(responses[val]) == num
                    for val, num in expected.items())
                    and (expect_blocking is False or len(blocking) > 0)):
                break
        else:
            assert False, f"Timed out, responses: {responses}."

        return responses, blocking

    client.deploy(name, v1, version="1", config={"num_replicas": 2})
    responses1, _ = make_nonblocking_calls({"1": 2})
    pids1 = responses1["1"]

    # ref2 will block a single replica until the signal is sent. Check that
    # some requests are now blocking.
    ref2 = call.remote(block=True)
    responses2, blocking2 = make_nonblocking_calls(
        {
            "1": 1
        }, expect_blocking=True)
    assert list(responses2["1"])[0] in pids1

    # Redeploy new version. Since there is one replica blocking, only one new
    # replica should be started up.
    goal_ref = client.deploy(
        name,
        v2,
        version="2",
        config={"num_replicas": 2},
        _blocking=False,
    )
    assert not client._wait_for_goal(goal_ref, timeout=0.1)
    responses3, blocking3 = make_nonblocking_calls(
        {
            "1": 1
        }, expect_blocking=True)

    # Signal the original call to exit.
    ray.get(signal.send.remote())
    val, pid = ray.get(ref2)
    assert val == "1"
    assert pid in responses1["1"]

    # Now the goal and requests to the new version should complete.
    # We should have two running replicas of the new version.
    assert client._wait_for_goal(goal_ref)
    make_nonblocking_calls({"2": 2})


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
