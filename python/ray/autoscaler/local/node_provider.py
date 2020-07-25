from filelock import FileLock
from threading import RLock
import json
import os
import socket
import logging
from http.client import RemoteDisconnected

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.local.config import bootstrap_local
from ray.autoscaler.tags import (
    TAG_RAY_NODE_TYPE,
    TAG_RAY_CLUSTER_NAME,
    NODE_TYPE_WORKER,
    NODE_TYPE_HEAD,
)

logger = logging.getLogger(__name__)

filelock_logger = logging.getLogger("filelock")
filelock_logger.setLevel(logging.WARNING)


class ClusterState:
    def __init__(self, lock_path, save_path, provider_config):
        self.lock = RLock()
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path):
                    workers = json.loads(open(self.save_path).read())
                    head_config = workers.get(provider_config["head_ip"])
                    if (not head_config or
                            head_config.get("tags", {}).get(TAG_RAY_NODE_TYPE)
                            != NODE_TYPE_HEAD):
                        workers = {}
                        logger.info("Head IP changed - recreating cluster.")
                else:
                    workers = {}
                logger.info("ClusterState: "
                            "Loaded cluster state: {}".format(list(workers)))
                for worker_ip in provider_config["worker_ips"]:
                    if worker_ip not in workers:
                        workers[worker_ip] = {
                            "tags": {
                                TAG_RAY_NODE_TYPE: NODE_TYPE_WORKER
                            },
                            "state": "terminated",
                        }
                    else:
                        assert (workers[worker_ip]["tags"][TAG_RAY_NODE_TYPE]
                                == NODE_TYPE_WORKER)
                if provider_config["head_ip"] not in workers:
                    workers[provider_config["head_ip"]] = {
                        "tags": {
                            TAG_RAY_NODE_TYPE: NODE_TYPE_HEAD
                        },
                        "state": "terminated",
                    }
                else:
                    assert (workers[provider_config["head_ip"]]["tags"][
                        TAG_RAY_NODE_TYPE] == NODE_TYPE_HEAD)
                # Ameer: relevant when a user reduces the number of workers
                # without changing the headnode.
                list_of_node_ips = list(provider_config["worker_ips"])
                list_of_node_ips.append(provider_config["head_ip"])
                for worker_ip in list(workers):
                    if worker_ip not in list_of_node_ips:
                        del workers[worker_ip]

                assert len(workers) == len(provider_config["worker_ips"]) + 1
                with open(self.save_path, "w") as f:
                    logger.debug("ClusterState: "
                                 "Writing cluster state: {}".format(workers))
                    f.write(json.dumps(workers))

    def get(self):
        with self.lock:
            with self.file_lock:
                workers = json.loads(open(self.save_path).read())
                return workers

    def put(self, worker_id, info):
        assert "tags" in info
        assert "state" in info
        with self.lock:
            with self.file_lock:
                workers = self.get()
                workers[worker_id] = info
                with open(self.save_path, "w") as f:
                    logger.info("ClusterState: "
                                "Writing cluster state: {}".format(
                                    list(workers)))
                    f.write(json.dumps(workers))


class OnPremCoordinatorState(ClusterState):
    def __init__(self, lock_path, save_path, list_of_node_ips):
        self.lock = RLock()
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path):
                    nodes = json.loads(open(self.save_path).read())
                else:
                    nodes = {}
                logger.info(
                    "OnPremCoordinatorState: "
                    "Loaded on prem coordinator state: {}".format(nodes))

                # Ameer: filter removed node ips
                for node_ip in list(nodes):
                    if node_ip not in list_of_node_ips:
                        del nodes[node_ip]

                for node_ip in list_of_node_ips:
                    if node_ip not in nodes:
                        nodes[node_ip] = {
                            "state": "terminated",
                        }

                assert len(nodes) == len(list_of_node_ips)
                with open(self.save_path, "w") as f:
                    logger.info(
                        "OnPremCoordinatorState: "
                        "Writing on prem coordinator state: {}".format(nodes))
                    f.write(json.dumps(nodes))


class LocalNodeProvider(NodeProvider):
    """NodeProvider for private/local clusters.

    `node_id` is overloaded to also be `node_ip` in this class.
    When `cluster_name` is None, it coordinates multiple clusters.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        if cluster_name:
            self.state = ClusterState(
                "/tmp/cluster-{}.lock".format(cluster_name),
                "/tmp/cluster-{}.state".format(cluster_name),
                provider_config,
            )
        else:
            # Local node provider with a coordinator server.
            self.state = OnPremCoordinatorState(
                "/tmp/coordinator-state.lock", "/tmp/coordinator-state.state",
                provider_config["list_of_node_ips"])

    def non_terminated_nodes(self, tag_filters):
        workers = self.state.get()
        matching_ips = []
        for worker_ip, info in workers.items():
            if info["state"] == "terminated":
                continue
            ok = True
            for k, v in tag_filters.items():
                if info["tags"].get(k) != v:
                    ok = False
                    break
            if ok:
                matching_ips.append(worker_ip)
        return matching_ips

    def is_running(self, node_id):
        return self.state.get()[node_id]["state"] == "running"

    def is_terminated(self, node_id):
        return not self.is_running(node_id)

    def node_tags(self, node_id):
        return self.state.get()[node_id]["tags"]

    def external_ip(self, node_id):
        return socket.gethostbyname(node_id)

    def internal_ip(self, node_id):
        return socket.gethostbyname(node_id)

    def set_node_tags(self, node_id, tags):
        with self.state.file_lock:
            info = self.state.get()[node_id]
            info["tags"].update(tags)
            self.state.put(node_id, info)

    def create_node(self, node_config, tags, count):
        node_type = tags[TAG_RAY_NODE_TYPE]
        with self.state.file_lock:
            workers = self.state.get()
            for node_id, info in workers.items():
                if (info["state"] == "terminated"
                        and info["tags"][TAG_RAY_NODE_TYPE] == node_type):
                    info["tags"] = tags
                    info["state"] = "running"
                    self.state.put(node_id, info)
                    return

    def terminate_node(self, node_id):
        workers = self.state.get()
        info = workers[node_id]
        info["state"] = "terminated"
        self.state.put(node_id, info)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_local(cluster_config)


class CoordinatorSenderNodeProvider(NodeProvider):
    """NodeProvider for automatically managed private/local clusters.

    The cluster management is handled by a coordinating server.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.coordinator_address = provider_config["coordinator_address"]

    def _get_http_response(self, request):
        headers = {
            "Content-Type": "application/json",
        }
        request_message = json.dumps(request).encode()
        http_coordinator_address = "http://" + self.coordinator_address

        try:
            import requests  # `requests` is not part of stdlib.
            from requests.exceptions import ConnectionError

            r = requests.get(
                http_coordinator_address,
                data=request_message,
                headers=headers,
                timeout=None,
            )
        except (RemoteDisconnected, ConnectionError):
            logger.exception("Could not connect to: " +
                             http_coordinator_address +
                             ". Did you run python coordinator_server.py" +
                             " --ips <list_of_node_ips> --port <PORT>?")
            raise
        except ImportError:
            logger.exception("Couldn't import `requests` library. "
                             "Be sure to install it on the client side.")
            raise

        response = r.json()
        return response

    def non_terminated_nodes(self, tag_filters):
        # Only get the non terminated nodes associated with this cluster name.
        tag_filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        request = {"type": "non_terminated_nodes", "args": (tag_filters, )}
        return self._get_http_response(request)

    def is_running(self, node_id):
        request = {"type": "is_running", "args": (node_id, )}
        return self._get_http_response(request)

    def is_terminated(self, node_id):
        request = {"type": "is_terminated", "args": (node_id, )}
        return self._get_http_response(request)

    def node_tags(self, node_id):
        request = {"type": "node_tags", "args": (node_id, )}
        return self._get_http_response(request)

    def external_ip(self, node_id):
        request = {"type": "external_ip", "args": (node_id, )}
        response = self._get_http_response(request)
        return response

    def internal_ip(self, node_id):
        request = {"type": "internal_ip", "args": (node_id, )}
        response = self._get_http_response(request)
        return response

    def create_node(self, node_config, tags, count):
        # Tag the newly created node with with this cluster name. Helps
        # to get the right nodes when calling non_terminated_nodes.
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        request = {
            "type": "create_node",
            "args": (node_config, tags, count),
        }
        self._get_http_response(request)

    def set_node_tags(self, node_id, tags):
        request = {"type": "set_node_tags", "args": (node_id, tags)}
        self._get_http_response(request)

    def terminate_node(self, node_id):
        request = {"type": "terminate_node", "args": (node_id, )}
        self._get_http_response(request)

    def terminate_nodes(self, node_ids):
        request = {"type": "terminate_nodes", "args": (node_ids, )}
        self._get_http_response(request)
