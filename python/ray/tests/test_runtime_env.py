import os
import pytest
import sys
import unittest

import tempfile
from pathlib import Path
import ray
from ray.test_utils import (run_string_as_driver,
                            run_string_as_driver_nonblocking)
from time import sleep
driver_script = """
from time import sleep
import sys
import logging
sys.path.insert(0, "{working_dir}")
import test_module
import ray
import ray.util
import os

job_config = ray.job_config.JobConfig(
    runtime_env={runtime_env}
)

if not job_config.runtime_env:
    job_config=None

try:
    if os.environ.get("USE_RAY_CLIENT"):
        ray.util.connect("{address}", job_config=job_config)
    else:
        ray.init(address="{address}",
                 job_config=job_config,
                 logging_level=logging.DEBUG)
except:
    print("ERROR")
    sys.exit(0)

@ray.remote
def run_test():
    return test_module.one()

@ray.remote
def check_file(name):
    try:
        with open(name) as f:
            return f.read()
    except:
        return "FAILED"

@ray.remote
class TestActor(object):
    @ray.method(num_returns=1)
    def one(self):
        return test_module.one()

{execute_statement}

if os.environ.get("USE_RAY_CLIENT"):
    ray.util.disconnect()
else:
    ray.shutdown()
sleep(10)
"""


@pytest.fixture(scope="function")
def working_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        module_path = path / "test_module"
        module_path.mkdir(parents=True)
        init_file = module_path / "__init__.py"
        test_file = module_path / "test.py"
        with test_file.open(mode="w") as f:
            f.write("""
def one():
    return 1
""")
        with init_file.open(mode="w") as f:
            f.write("""
from test_module.test import one
""")
        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield tmp_dir
        os.chdir(old_dir)


def start_client_server(cluster, client_mode):
    from ray._private.runtime_env import PKG_DIR
    if not client_mode:
        return (cluster.address, None, PKG_DIR)
    ray.worker._global_node._ray_params.ray_client_server_port = "10003"
    ray.worker._global_node.start_ray_client_server()
    return ("localhost:10003", {"USE_RAY_CLIENT": "1"}, PKG_DIR)


"""
The following test cases are related with runtime env. It following these steps
  1) Creating a temporary dir with fixture working_dir
  2) Using a template named driver_script defined globally
  3) Overwrite runtime_env and execute_statement in the template
  4) Execute it as a separate driver and return the result
"""


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_single_node(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # Setup runtime env here
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # Testing runtime env with working_dir
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_module(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # test runtime_env iwth py_modules
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_local_file(two_node_cluster, working_dir, client_mode):
    with open(os.path.join(working_dir, "test_file"), "w") as f:
        f.write("1")
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    # test runtime_env iwth working_dir
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
vals = ray.get([check_file.remote('test_file')] * 1000)
print(sum([int(v) for v in vals]))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_exclusion(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    working_path = Path(working_dir)

    def create_file(p):
        if not p.parent.exists():
            p.parent.mkdir()
        with p.open("w") as f:
            f.write("Test")

    create_file(working_path / "tmp_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "test_2")
    create_file(working_path / "tmp_dir" / "test_3")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_1")
    create_file(working_path / "tmp_dir" / "sub_dir" / "test_2")
    create_file(working_path / "test1")
    create_file(working_path / "test2")
    create_file(working_path / "test3")
    tmp_dir_test_3 = str((working_path / "tmp_dir" / "test_3").absolute())
    runtime_env = f"""{{
        "working_dir": r"{working_dir}",
    }}"""
    execute_statement = """
    vals = ray.get([
        check_file.remote('test1'),
        check_file.remote('test2'),
        check_file.remote('test3'),
        check_file.remote(os.path.join('tmp_dir', 'test_1')),
        check_file.remote(os.path.join('tmp_dir', 'test_2')),
        check_file.remote(os.path.join('tmp_dir', 'test_3')),
        check_file.remote(os.path.join('tmp_dir', 'sub_dir', 'test_1')),
        check_file.remote(os.path.join('tmp_dir', 'sub_dir', 'test_2')),
    ])
    print(','.join(vals))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    # Test it works before
    assert out.strip().split("\n")[-1] == \
        "Test,Test,Test,Test,Test,Test,Test,Test"
    runtime_env = f"""{{
        "working_dir": r"{working_dir}",
        "excludes": [
            # exclude by absolute path
            r"{tmp_dir_test_3}",
            # exclude by relative path
            r"{str(working_path / "test2")}",
            # exclude by dir
            r"{str(working_path / "tmp_dir" / "sub_dir")}",
            # exclude part of the dir
            r"{str(working_path / "tmp_dir" / "test_1")}",
            # exclude part of the dir
            r"{str(working_path / "tmp_dir" / "test_2")}",
        ]
    }}"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    # Test it works before
    assert out.strip().split("\n")[-1] == \
        "Test,FAILED,Test,FAILED,FAILED,FAILED,FAILED,FAILED"


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_two_node_uri(two_node_cluster, working_dir, client_mode):
    cluster, _ = two_node_cluster
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    import ray._private.runtime_env as runtime_env
    import tempfile
    with tempfile.NamedTemporaryFile(suffix="zip") as tmp_file:
        pkg_name = runtime_env.get_project_package_name(working_dir, [], [])
        pkg_uri = runtime_env.Protocol.PIN_GCS.value + "://" + pkg_name
        runtime_env.create_project_package(working_dir, [], [], tmp_file.name)
        runtime_env.push_package(pkg_uri, tmp_file.name)
        runtime_env = f"""{{ "working_dir_uri": "{pkg_uri}" }}"""
        # Execute the following cmd in driver with runtime_env
        execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_regular_actors(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
test_actor = TestActor.options(name="test_actor").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
@pytest.mark.parametrize("client_mode", [True, False])
def test_detached_actors(ray_start_cluster_head, working_dir, client_mode):
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, client_mode)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the following cmd in driver with runtime_env
    execute_statement = """
test_actor = TestActor.options(name="test_actor", lifetime="detached").remote()
print(sum(ray.get([test_actor.one.remote()] * 1000)))
"""
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "1000"
    # It's a detached actors, so it should still be there
    assert len(list(Path(PKG_DIR).iterdir())) == 2
    pkg_dir = [f for f in Path(PKG_DIR).glob("*") if f.is_dir()][0]
    import sys
    sys.path.insert(0, str(pkg_dir))
    test_actor = ray.get_actor("test_actor")
    assert sum(ray.get([test_actor.one.remote()] * 1000)) == 1000
    ray.kill(test_actor)
    from time import sleep
    sleep(5)
    assert len(list(Path(PKG_DIR).iterdir())) == 1


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_jobconfig_compatible_1(ray_start_cluster_head, working_dir):
    # start job_config=None
    # start job_config=something
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, True)
    runtime_env = None
    # To make the first one hanging there
    execute_statement = """
sleep(600)
"""
    script = driver_script.format(**locals())
    # Have one running with job config = None
    proc = run_string_as_driver_nonblocking(script, env)
    # waiting it to be up
    sleep(5)
    runtime_env = f"""{{  "working_dir": "{working_dir}" }}"""
    # Execute the second one which should trigger an error
    execute_statement = "print(sum(ray.get([run_test.remote()] * 1000)))"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "ERROR"
    proc.kill()
    proc.wait()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_jobconfig_compatible_2(ray_start_cluster_head, working_dir):
    # start job_config=something
    # start job_config=None
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, True)
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # To make the first one hanging there
    execute_statement = """
sleep(600)
"""
    script = driver_script.format(**locals())
    proc = run_string_as_driver_nonblocking(script, env)
    sleep(5)
    runtime_env = None
    # Execute the following in the second one which should
    # succeed
    execute_statement = "print('OK')"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    assert out.strip().split()[-1] == "OK"
    proc.kill()
    proc.wait()


@unittest.skipIf(sys.platform == "win32", "Fail to create temp dir.")
def test_jobconfig_compatible_3(ray_start_cluster_head, working_dir):
    # start job_config=something
    # start job_config=something else
    cluster = ray_start_cluster_head
    (address, env, PKG_DIR) = start_client_server(cluster, True)
    runtime_env = """{  "py_modules": [test_module.__path__[0]] }"""
    # To make the first one hanging ther
    execute_statement = """
sleep(600)
"""
    script = driver_script.format(**locals())
    proc = run_string_as_driver_nonblocking(script, env)
    sleep(5)
    runtime_env = f"""{{  "working_dir": test_module.__path__[0] }}"""
    # Execute the following cmd in the second one which should
    # fail
    execute_statement = "print('OK')"
    script = driver_script.format(**locals())
    out = run_string_as_driver(script, env)
    proc.kill()
    proc.wait()
    assert out.strip().split()[-1] == "ERROR"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
