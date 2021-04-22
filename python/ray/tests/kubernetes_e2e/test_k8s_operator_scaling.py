"""
Tests scaling behavior of Kubernetes operator.


(1) Start a cluster with minWorkers = 30 and verify scale-up
(2) Edit minWorkers to 0, verify scale-down
(3) Submit a task requiring 14 workers using Ray client
(4) Verify scale-up, task execution, and scale down.
"""
import copy
import kubernetes
import subprocess
import sys
import tempfile
import time
import unittest

import pytest
import yaml

import ray
from test_k8s_operator_examples import get_operator_config_path
from test_k8s_operator_examples import retry_until_true
from test_k8s_operator_examples import wait_for_pods
from test_k8s_operator_examples import IMAGE
from test_k8s_operator_examples import PULL_POLICY
from test_k8s_operator_examples import NAMESPACE


def submit_scaling_job(client_port, num_tasks):
    @ray.remote(num_cpus=1)
    def f(i):
        time.sleep(60)
        return i

    print(">>>Submitting tasks with Ray client.")
    ray.util.connect(f"127.0.0.1:{client_port}")
    futures = [f.remote(i) for i in range(num_tasks)]

    print(">>>Verifying scale-up.")
    # Expect as many pods as tasks.
    # (each Ray pod has 1 CPU)
    wait_for_pods(num_tasks)

    print(">>>Waiting for task output.")
    task_output = ray.get(futures, timeout=360)

    assert task_output == list(range(num_tasks)), "Tasks did not"\
        "complete with expected output."


@retry_until_true
def wait_for_operator():
    cmd = "kubectl get pods"
    out = subprocess.check_output(cmd, shell=True).decode()
    for line in out.splitlines():
        if "ray-operator" in line and "Running" in line:
            return True
    return False


class KubernetesScaleTest(unittest.TestCase):
    def test_scaling(self):
        with tempfile.NamedTemporaryFile("w+") as example_cluster_file:

            example_cluster_config_path = get_operator_config_path(
                "example_cluster.yaml")

            crd_path = get_operator_config_path(
                "cluster_crd.yaml")

            example_cluster_config = yaml.safe_load(
                open(example_cluster_config_path).read())

            # Set image and pull policy
            podTypes = example_cluster_config["spec"]["podTypes"]
            pod_specs = [
                podType["podConfig"]["spec"] for podType in podTypes
            ]
            for pod_spec in pod_specs:
                pod_spec["containers"][0]["image"] = IMAGE
                pod_spec["containers"][0]["imagePullPolicy"] = PULL_POLICY

            # Config set-up for this test.
            example_cluster_config["spec"]["maxWorkers"] = 100
            example_cluster_config["spec"]["idleTimeoutMinutes"] = 1
            worker_type = podTypes[1]
            # Make sure we have the right type
            assert "worker" in worker_type["name"]
            worker_type["maxWorkers"] = 100
            # Key for the first part of this test:
            worker_type["minWorkers"] = 30

            yaml.dump(example_cluster_config, example_cluster_file)

            example_cluster_file.flush()

            # Test creating operator before CRD.
            print(">>>Waiting for Ray operator to enter running state.")
            wait_for_operator()

            print(">>>Creating RayCluster CRD.")
            cmd = f"kubectl apply -f {crd_path}"
            subprocess.check_call(cmd, shell=True)

            # Start a 30-pod-cluster.
            print(">>>Starting a cluster.")
            cd = f"kubectl -n {NAMESPACE} apply -f {example_cluster_file.name}"
            subprocess.check_call(cd, shell=True)

            # Check that autoscaling respects minWorkers by waiting for
            # 32 pods in the namespace.
            print(">>>Waiting for pods to join cluster.")
            wait_for_pods(31)

            # Check scale-down.
            print(">>>Decreasing min workers to 0.")
            example_cluster_edit = copy.deepcopy(example_cluster_config)
            # Set minWorkers to 0:
            example_cluster_edit["spec"]["podTypes"][1]["minWorkers"] = 0
            yaml.dump(example_cluster_edit, example_cluster_file)
            example_cluster_file.flush()
            cm = f"kubectl -n {NAMESPACE} apply -f {example_cluster_file.name}"
            subprocess.check_call(cm, shell=True)
            print(">>>Sleeping for a minute while workers time-out.")
            time.sleep(60)
            print(">>>Verifying scale-down.")
            wait_for_pods(1)

            # Test scale up and scale down after task submission.
            command = f"kubectl -n {NAMESPACE}"\
                " port-forward service/example-cluster-ray-head 10001:10001"
            command = command.split()
            print(">>>Port-forwarding head service.")
            self.proc = subprocess.Popen(command)
            try:
                # Wait a bit for the port-forwarding connection to be
                # established.
                time.sleep(10)
                # Check that job submission works
                submit_scaling_job(client_port="10001", num_tasks=15)
                # Clean up
                self.proc.kill()
            except Exception as e:
                # Clean up on failure
                self.proc.kill()
                raise (e)

            print(">>>Sleeping for a minute while workers time-out.")
            time.sleep(60)
            print(">>>Verifying scale-down.")
            wait_for_pods(1)

    def __del__(self):
        # To be safer, kill again:
        # (does not raise an error if the process has already been killed)
        self.proc.kill()


if __name__ == "__main__":
    kubernetes.config.load_kube_config()
    sys.exit(pytest.main(["-sv", __file__]))
