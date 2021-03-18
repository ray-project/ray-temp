import os
import sys
import copy
import time
import logging
import requests
import tempfile
import zipfile
import hashlib
import shutil
import traceback
import subprocess

import ray
from ray._private.utils import hex_to_binary
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.new_dashboard.modules.job import md5sum
import pytest

logger = logging.getLogger(__name__)

JOB_ROOT_DIR = "/tmp/ray/job"
TEST_PYTHON_JOB = {
    "name": "Test job",
    "owner": "abc.xyz",
    "language": "PYTHON",
    "url": "http://xxx/yyy.zip",
    "driverEntry": "python_file_name_without_ext",
    "driverArgs": [],
    "customConfig": {
        "k1": "v1",
        "k2": "v2"
    },
    "jvmOptions": [],
    "dependencies": {
        "python": [
            "py-spy >= 0.2.0",
        ],
        "java": [{
            "name": "spark",
            "version": "2.1",
            "url": "<invalid url>",
            "md5": "<md5 hex>"
        }]
    }
}

TEST_PYTHON_JOB_CODE = """
import os
import sys
import ray
import time


@ray.remote
class Actor:
    def __init__(self, index):
        self._index = index

    def foo(self, x):
        print("worker job dir {}".format(os.environ["RAY_JOB_DIR"]))
        print(f"worker cwd {os.getcwd()}")
        assert os.path.samefile(os.environ["RAY_JOB_DIR"], os.getcwd())
        assert os.environ["RAY_JOB_DIR"] in sys.path
        return f"Actor {self._index}: {x}"


def main():
    actors = []
    print("driver job dir {}".format(os.environ["RAY_JOB_DIR"]))
    print(f"driver cwd {os.getcwd()}")
    assert os.path.samefile(os.environ["RAY_JOB_DIR"], os.getcwd())
    assert os.environ["RAY_JOB_DIR"] in sys.path
    for x in range(2):
        actors.append(Actor.remote(x))

    counter = 0
    while True:
        for a in actors:
            r = a.foo.remote(counter)
            print(ray.get(r))
            counter += 1
            time.sleep(1)


if __name__ == "__main__":
    ray.init()
    main()
"""


def _gen_job_zip(job_code, driver_entry):
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        with zipfile.ZipFile(f, mode="w") as zip_f:
            with zip_f.open(f"{driver_entry}.py", "w") as driver:
                driver.write(job_code.encode())
        return f.name


def _gen_md5(path):
    hash_md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def _gen_url(weburl, path):
    return f"{weburl}/test/file?path={path}"


def _get_python_job(web_url,
                    java_dependency_url=None,
                    java_dependency_md5=None):
    driver_entry = "simple_job"
    path = _gen_job_zip(TEST_PYTHON_JOB_CODE, driver_entry)
    url = _gen_url(web_url, path)
    job = copy.deepcopy(TEST_PYTHON_JOB)
    job["url"] = url
    job["driverEntry"] = driver_entry
    if java_dependency_url:
        job["dependencies"]["java"][0]["url"] = java_dependency_url
    if java_dependency_md5:
        job["dependencies"]["java"][0]["md5"] = java_dependency_md5
    return job


def test_md5sum():
    cmd = [sys.executable, md5sum.__file__, md5sum.__file__]
    r = subprocess.check_output(cmd)
    assert r.split()[0].strip() == b"7d9c76a4fa26506dea45624656eb55a3"


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "job_config": ray.job_config.JobConfig(code_search_path=[""]),
    }],
    indirect=True)
def test_submit_job_with_invalid_url(disable_aiohttp_cache, enable_test_module,
                                     ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs", json=_get_python_job(webui_url))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 2

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "FAILED", job_info["failErrorMessage"]
            assert "InvalidURL" in job_info["failErrorMessage"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_error, timeout=20)


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "job_config": ray.job_config.JobConfig(code_search_path=[""]),
    }],
    indirect=True)
def test_submit_job_with_incorrect_md5(
        disable_aiohttp_cache, enable_test_module, ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url, java_dependency_url=fake_jar_url))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 2

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_error():
        try:
            resp = requests.get(webui_url + f"/jobs/{job_id}")
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_info = result["data"]["detail"]["jobInfo"]
            assert job_info["state"] == "FAILED", job_info["failErrorMessage"]
            assert "is corrupted" in job_info["failErrorMessage"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_error, timeout=20)


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "job_config": ray.job_config.JobConfig(code_search_path=[""]),
    }],
    indirect=True)
def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    shutil.rmtree(JOB_ROOT_DIR, ignore_errors=True)

    fake_jar_url = _gen_url(webui_url, __file__)
    fake_jar_md5 = _gen_md5(__file__)

    job_id = None

    def _submit_job():
        try:
            resp = requests.post(
                webui_url + "/jobs",
                json=_get_python_job(
                    webui_url,
                    java_dependency_url=fake_jar_url,
                    java_dependency_md5=fake_jar_md5))
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            nonlocal job_id
            job_id = result["data"]["jobId"]
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_submit_job, 5)

    resp = requests.get(webui_url + "/jobs?view=summary")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    summary = result["data"]["summary"]
    assert len(summary) == 2

    resp = requests.get(webui_url + f"/jobs/{job_id}")
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is True, resp.text
    job_info = result["data"]["detail"]["jobInfo"]
    assert job_info["name"] == "Test job"
    assert job_info["jobId"] == job_id

    def _check_running():
        resp = requests.get(webui_url + f"/jobs/{job_id}")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_info = result["data"]["detail"]["jobInfo"]
        assert job_info["state"] == "RUNNING", job_info["failErrorMessage"]
        job_actors = result["data"]["detail"]["jobActors"]
        job_workers = result["data"]["detail"]["jobWorkers"]
        assert len(job_actors) > 0
        assert len(job_workers) > 0

    timeout_seconds = 60
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(5)
        try:
            _check_running()
            break
        except (AssertionError, KeyError, IndexError) as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_get_job_info(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actor = Actor.remote()
    actor_pid = ray.get(actor.getpid.remote())
    actor_id = actor._actor_id.hex()

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    ip = ray._private.services.get_node_ip_address()

    def _check():
        resp = requests.get(f"{webui_url}/jobs?view=summary")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_summary = result["data"]["summary"]
        assert len(job_summary) == 1, resp.text
        one_job = job_summary[0]
        assert "jobId" in one_job
        job_id = one_job["jobId"]
        assert ray._raylet.JobID(hex_to_binary(one_job["jobId"]))
        assert "driverIpAddress" in one_job
        assert one_job["driverIpAddress"] == ip
        assert "driverPid" in one_job
        assert one_job["driverPid"] == str(os.getpid())
        assert "config" in one_job
        assert type(one_job["config"]) is dict
        assert "isDead" in one_job
        assert one_job["isDead"] is False
        assert "timestamp" in one_job
        one_job_summary_keys = one_job.keys()

        resp = requests.get(f"{webui_url}/jobs/{job_id}")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_detail = result["data"]["detail"]
        assert "jobInfo" in job_detail
        assert len(one_job_summary_keys - job_detail["jobInfo"].keys()) == 0
        assert "jobActors" in job_detail
        job_actors = job_detail["jobActors"]
        assert len(job_actors) == 1, resp.text
        one_job_actor = job_actors[actor_id]
        assert "taskSpec" in one_job_actor
        assert type(one_job_actor["taskSpec"]) is dict
        assert "functionDescriptor" in one_job_actor["taskSpec"]
        assert type(one_job_actor["taskSpec"]["functionDescriptor"]) is dict
        assert "pid" in one_job_actor
        assert one_job_actor["pid"] == actor_pid
        check_actor_keys = [
            "name", "timestamp", "address", "actorId", "jobId", "state"
        ]
        for k in check_actor_keys:
            assert k in one_job_actor
        assert "jobWorkers" in job_detail
        job_workers = job_detail["jobWorkers"]
        assert len(job_workers) == 1, resp.text
        one_job_worker = job_workers[0]
        check_worker_keys = [
            "cmdline", "pid", "cpuTimes", "memoryInfo", "cpuPercent",
            "coreWorkerStats", "language", "jobId"
        ]
        for k in check_worker_keys:
            assert k in one_job_worker

    timeout_seconds = 30
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(5)
        try:
            _check()
            break
        except (AssertionError, KeyError, IndexError) as ex:
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
