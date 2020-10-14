import os
import sys
import logging
import requests
import time
import traceback
import pytest
import ray
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
)

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


def test_actor_groups(ray_start_with_dashboard):
    @ray.remote
    class Foo:
        def __init__(self, num):
            print("Starting up printed")
            self.num = num

        def do_task(self):
            print("printing returning num")
            raise ValueError("UH OH!!!!")

    @ray.remote(num_gpus=1)
    class InfeasibleActor:
        pass

    foo_actor = Foo.remote(4)
    infeasible_actor = InfeasibleActor.remote()  # noqa
    try:
        result = ray.get(foo_actor.do_task.remote())  # noqa
    except ValueError:
        pass
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 20
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(1)
        try:
            response = requests.get(webui_url + "/logical/actor_groups")
            response.raise_for_status()
            actor_groups_resp = response.json()
            assert actor_groups_resp["result"] is True, actor_groups_resp[
                "msg"]
            actor_groups = actor_groups_resp["data"]["actorGroups"]
            assert "Foo" in actor_groups
            summary = actor_groups["Foo"]["summary"]
            # 2 __init__ tasks and 2 do_task tasks
            assert summary["numExecutedTasks"] == 4
            assert summary["stateToCount"]["ALIVE"] == 2
            assert summary["numLogs"] == 1
            assert summary["numErrors"] == 1

            entries = actor_groups["Foo"]["entries"]
            assert len(entries) == 1
            assert "logs" in entries[0]
            assert "errors" in entries[0]
            assert len(entries[0]["logs"]) == 1
            assert len(entries[0]["errors"]) == 1

            assert "InfeasibleActor" in actor_groups
            entries = actor_groups["InfeasibleActor"]["entries"]
            assert "requiredResources" in entries[0]
            assert "GPU" in entries[0]["requiredResources"]
            break
        except Exception as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
