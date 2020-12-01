"""
Ray operator for Kubernetes.

Reads ray cluster config from a k8s ConfigMap, starts a ray head node pod using
create_or_update_cluster(), then runs an autoscaling loop in the operator pod
executing this script. Writes autoscaling logs to the directory
/root/ray-operator-logs.

In this setup, the ray head node does not run an autoscaler. It is important
NOT to supply an --autoscaling-config argument to head node's ray start command
in the cluster config when using this operator.

To run, first create a ConfigMap named ray-operator-configmap from a ray
cluster config. Then apply the manifest at python/ray/autoscaler/kubernetes/operator_configs/operator_config.yaml

For example:
kubectl create namespace raytest
kubectl -n raytest create configmap ray-operator-configmap --from-file=python/ray/autoscaler/kubernetes/operator_configs/test_cluster_config.yaml
kubectl -n raytest apply -f python/ray/autoscaler/kubernetes/operator_configs/operator_config.yaml
""" # noqa
import logging
import multiprocessing as mp
import os
import yaml

from ray._private import services
from ray.autoscaler._private import commands
from ray import monitor
from ray.operator import operator_utils
from ray import ray_constants


class RayCluster():
    def __init__(self, config):
        self.config = config
        self.name = self.config["cluster_name"]
        self.config_path = operator_utils.config_path(self.name)

        self.setup_logging()

        self.subprocess = None

    def do_in_subprocess(self, f, wait_to_finish=False):
        # First stop the subprocess if it's alive
        if self.subprocess and self.subprocess.is_alive():
            self.subprocess.terminate()
            self.subprocess.join()
        # Reinstantiate process with f as target and start.
        self.subprocess = mp.Process(name=self.name, target=f)
        self.subprocess.start()
        if wait_to_finish:
            self.subprocess.join()

    def create_or_update(self):
        self.do_in_subprocess(self._create_or_update)

    def _create_or_update(self):
        self.start_head()
        self.start_monitor()

    def start_head(self):
        self.write_config()
        self.config = commands.create_or_update_cluster(
            self.config_path,
            override_min_workers=None,
            override_max_workers=None,
            no_restart=False,
            restart_only=False,
            yes=True,
            no_config_cache=True)
        self.write_config()

    def write_config(self):
        with open(self.config_path, "w") as file:
            yaml.dump(self.config, file)

    def start_monitor(self):
        ray_head_pod_ip = commands.get_head_node_ip(self.config_path)
        # TODO: Add support for user-specified redis port and password
        redis_address = services.address(ray_head_pod_ip,
                                         ray_constants.DEFAULT_PORT)
        self.mtr = monitor.Monitor(
            redis_address=redis_address,
            autoscaling_config=self.config_path,
            redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
            prefix_cluster_info=True)
        self.mtr.run()

    def tear_down(self):
        self.do_in_subprocess(self._tear_down, wait_to_finish=True)
        self.clean_up_logging()

    def _tear_down(self):
        commands.teardown_cluster(
            self.config_path,
            yes=True,
            workers_only=False,
            override_cluster_name=None,
            keep_min_workers=False)

    def setup_logging(self):
        self.handler = logging.StreamHandler()
        self.handler.addFilter(lambda rec: rec.processName == self.name)
        logging_format = ":".join([self.name, ray_constants.LOGGER_FORMAT])
        self.handler.setFormatter(logging.Formatter(logging_format))
        operator_utils.root_logger.addHandler(self.handler)

    def clean_up_logging(self):
        operator_utils.root_logger.removeHandler(self.handler)


ray_clusters = {}


def cluster_action(cluster_config, event_type):
    cluster_name = cluster_config["cluster_name"]
    if event_type == "ADDED":
        ray_clusters[cluster_name] = RayCluster(cluster_config)
        ray_clusters[cluster_name].create_or_update()
    elif event_type == "MODIFIED":
        ray_clusters[cluster_name].create_or_update()
    elif event_type == "DELETED":
        ray_clusters[cluster_name].tear_down()
        del ray_clusters[cluster_name]


def main():
    if not os.path.isdir(operator_utils.RAY_CONFIG_DIR):
        os.mkdir(operator_utils.RAY_CONFIG_DIR)
    stream = operator_utils.cluster_cr_stream()
    for event in stream:
        cluster_cr = event["object"]
        event_type = event["type"]
        cluster_config = operator_utils.cr_to_config(cluster_cr)
        cluster_action(cluster_config, event_type)


if __name__ == "__main__":
    main()
