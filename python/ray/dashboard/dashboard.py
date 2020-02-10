try:
    import aiohttp.web
except ImportError:
    print("The dashboard requires aiohttp to run.")
    import sys
    sys.exit(1)

import copy
import datetime
import json
import logging
import os
import re
import threading
import time
import traceback
import yaml
import uuid

from base64 import b64decode
from collections import defaultdict
from operator import itemgetter
from typing import Dict

import grpc
from google.protobuf.json_format import MessageToDict
import ray

from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
from ray.dashboard.hosted_dashboard.dashboard_client import DashboardClient
from ray.dashboard.dashboard_controller_interface \
    import DashboardControllerInterface

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


def to_unix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def round_resource_value(quantity):
    if quantity.is_integer():
        return int(quantity)
    else:
        return round(quantity, 2)


def format_resource(resource_name, quantity):
    if resource_name == "object_store_memory" or resource_name == "memory":
        # Convert to 50MiB chunks and then to GiB
        quantity = quantity * (50 * 1024 * 1024) / (1024 * 1024 * 1024)
        return "{} GiB".format(round_resource_value(quantity))
    return "{}".format(round_resource_value(quantity))


def format_reply_id(reply):
    if isinstance(reply, dict):
        for k, v in reply.items():
            if isinstance(v, dict) or isinstance(v, list):
                format_reply_id(v)
            else:
                if k.endswith("Id"):
                    v = b64decode(v)
                    reply[k] = ray.utils.binary_to_hex(v)
    elif isinstance(reply, list):
        for item in reply:
            format_reply_id(item)


def measures_to_dict(measures):
    measures_dict = {}
    for measure in measures:
        tags = measure["tags"].split(",")[-1]
        if "intValue" in measure:
            measures_dict[tags] = measure["intValue"]
        elif "doubleValue" in measure:
            measures_dict[tags] = measure["doubleValue"]
    return measures_dict


def b64_decode(reply):
    return b64decode(reply).decode("utf-8")


class DashboardController(DashboardControllerInterface):
    """DashboardController contains business logic for running dashboard."""

    def __init__(self, redis_address, redis_password, update_frequency=1.0):
        self.node_stats = NodeStats(redis_address, redis_password)
        self.raylet_stats = RayletStats(redis_address, update_frequency,
                                        redis_password)

    def _construct_raylet_info(self):
        D = self.raylet_stats.get_raylet_stats()
        workers_info_by_node = {
            data["nodeId"]: data.get("workersStats")
            for data in D.values()
        }
        infeasible_tasks = sum(
            (data.get("infeasibleTasks", []) for data in D.values()), [])
        actor_tree = self.node_stats.get_actor_tree(workers_info_by_node,
                                                    infeasible_tasks)
        for address, data in D.items():
            # process view data
            measures_dicts = {}
            for view_data in data["viewData"]:
                view_name = view_data["viewName"]
                if view_name in ("local_available_resource",
                                 "local_total_resource",
                                 "object_manager_stats"):
                    measures_dicts[view_name] = measures_to_dict(
                        view_data["measures"])
            # process resources info
            extra_info_strings = []
            prefix = "ResourceName:"
            for resource_name, total_resource in measures_dicts[
                    "local_total_resource"].items():
                available_resource = measures_dicts[
                    "local_available_resource"].get(resource_name, .0)
                resource_name = resource_name[len(prefix):]
                extra_info_strings.append("{}: {} / {}".format(
                    resource_name,
                    format_resource(resource_name,
                                    total_resource - available_resource),
                    format_resource(resource_name, total_resource)))
            data["extraInfo"] = ", ".join(extra_info_strings) + "\n"
            if os.environ.get("RAY_DASHBOARD_DEBUG"):
                # process object store info
                extra_info_strings = []
                prefix = "ValueType:"
                for stats_name in [
                        "used_object_store_memory", "num_local_objects"
                ]:
                    stats_value = measures_dicts["object_manager_stats"].get(
                        prefix + stats_name, .0)
                    extra_info_strings.append("{}: {}".format(
                        stats_name, stats_value))
                data["extraInfo"] += ", ".join(extra_info_strings)
                # process actor info
                actor_tree_str = json.dumps(
                    actor_tree, indent=2, sort_keys=True)
                lines = actor_tree_str.split("\n")
                max_line_length = max(map(len, lines))
                to_print = []
                for line in lines:
                    to_print.append(line + (max_line_length - len(line)) * " ")
                data["extraInfo"] += "\n" + "\n".join(to_print)
        return {"nodes": D, "actors": actor_tree}

    def node_info(self):
        return self.node_stats.get_node_stats()

    def raylet_info(self):
        return self._construct_raylet_info()

    def launch_profiling(self, node_id, pid, duration):
        profiling_id = self.raylet_stats.launch_profiling(
            node_id=node_id, pid=pid, duration=duration)
        return profiling_id

    def check_profiling_status(self, profiling_id):
        return self.raylet_stats.check_profiling_status(profiling_id)

    def get_profiling_info(self, profiling_id):
        return self.raylet_stats.get_profiling_info(profiling_id)

    def kill_actor(self, actor_id, ip_address, port):
        return self.raylet_stats.kill_actor(actor_id, ip_address, port)

    def logs(self, hostname, pid):
        return self.node_stats.get_logs(hostname, pid)

    def errors(self, hostname, pid):
        self.node_stats.get_errors(hostname, pid)

    def start_collecting_metrics(self):
        self.node_stats.start()
        self.raylet_stats.start()


class Dashboard(object):
    """A dashboard process for monitoring Ray nodes.

    This dashboard is made up of a REST API which collates data published by
        Reporter processes on nodes into a json structure, and a webserver
        which polls said API for display purposes.

    Args:
        host(str): Host address of dashboard aiohttp server.
        port(str): Port number of dashboard aiohttp server.
        redis_address(str): GCS address of a Ray cluster
        temp_dir (str): The temporary directory used for log files and
            information for this Ray session.
        redis_passord(str): Redis password to access GCS
        hosted_dashboard_client(bool): True if a server runs as a
            hosted dashboard client mode. 
        update_frequency(float): Frequency where metrics are updated.
        DashboardController(DashboardController): DashboardController
            that defines the business logic of a Dashboard server.
    """

    def __init__(self,
                 host,
                 port,
                 redis_address,
                 temp_dir,
                 redis_password=None,
                 hosted_dashboard_client=True,
                 update_frequency=1.0,
                 DashboardController=DashboardController):
        self.host = host
        self.port = port
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)
        self.temp_dir = temp_dir

        self.dashboard_controller = DashboardController(
            redis_address, redis_password, update_frequency)

        self.hosted_dashboard_client = hosted_dashboard_client
        self.dashboard_client = None 
        if self.hosted_dashboard_client:
            self.dashboard_client = DashboardClient(self.dashboard_controller)

        # Setting the environment variable RAY_DASHBOARD_DEV=1 disables some
        # security checks in the dashboard server to ease development while
        # using the React dev server. Specifically, when this option is set, we
        # allow cross-origin requests to be made.
        self.is_dev = os.environ.get("RAY_DASHBOARD_DEV") == "1"

        self.app = aiohttp.web.Application()
        self.setup_routes()

    def setup_routes(self):
        def forbidden() -> aiohttp.web.Response:
            return aiohttp.web.Response(status=403, text="403 Forbidden")

        def get_forbidden(_) -> aiohttp.web.Response:
            return forbidden()

        async def get_index(req) -> aiohttp.web.Response:
            return aiohttp.web.FileResponse(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "client/build/index.html"))

        async def get_favicon(req) -> aiohttp.web.Response:
            return aiohttp.web.FileResponse(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "client/build/favicon.ico"))

        async def json_response(result=None, error=None,
                                ts=None) -> aiohttp.web.Response:
            if ts is None:
                ts = datetime.datetime.utcnow()

            headers = None
            if self.is_dev:
                headers = {"Access-Control-Allow-Origin": "*"}

            return aiohttp.web.json_response(
                {
                    "result": result,
                    "timestamp": to_unix_time(ts),
                    "error": error,
                },
                headers=headers)

        async def ray_config(_) -> aiohttp.web.Response:
            try:
                config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
            except Exception:
                return await json_response(error="No config")

            D = {
                "min_workers": cfg["min_workers"],
                "max_workers": cfg["max_workers"],
                "initial_workers": cfg["initial_workers"],
                "autoscaling_mode": cfg["autoscaling_mode"],
                "idle_timeout_minutes": cfg["idle_timeout_minutes"],
            }

            try:
                D["head_type"] = cfg["head_node"]["InstanceType"]
            except KeyError:
                D["head_type"] = "unknown"

            try:
                D["worker_type"] = cfg["worker_nodes"]["InstanceType"]
            except KeyError:
                D["worker_type"] = "unknown"

            return await json_response(result=D)

        async def node_info(req) -> aiohttp.web.Response:
            now = datetime.datetime.utcnow()
            D = self.dashboard_controller.node_info()
            return await json_response(result=D, ts=now)

        async def raylet_info(req) -> aiohttp.web.Response:
            result = self.dashboard_controller.raylet_info()
            return await json_response(result=result)

        async def launch_profiling(req) -> aiohttp.web.Response:
            node_id = req.query.get("node_id")
            pid = int(req.query.get("pid"))
            duration = int(req.query.get("duration"))
            profiling_id = self.dashboard_controller.launch_profiling(
                node_id, pid, duration)
            return await json_response(str(profiling_id))

        async def check_profiling_status(req) -> aiohttp.web.Response:
            profiling_id = req.query.get("profiling_id")
            status = self.dashboard_controller.check_profiling_status(
                profiling_id)
            return await json_response(result=status)

        async def get_profiling_info(req) -> aiohttp.web.Response:
            profiling_id = req.query.get("profiling_id")
            profiling_info = self.dashboard_controller.get_profiling_info(
                profiling_id)
            return aiohttp.web.json_response(profiling_info)

        async def kill_actor(req) -> aiohttp.web.Response:
            actor_id = req.query.get("actor_id")
            ip_address = req.query.get("ip_address")
            port = req.query.get("port")
            return await json_response(
                self.dashboard_controller.kill_actor(actor_id, ip_address,
                                                     port))

        async def logs(req) -> aiohttp.web.Response:
            hostname = req.query.get("hostname")
            pid = req.query.get("pid")
            result = self.dashboard_controller.logs(hostname, pid)
            return await json_response(result=result)

        async def errors(req) -> aiohttp.web.Response:
            hostname = req.query.get("hostname")
            pid = req.query.get("pid")
            result = self.dashboard_controller.errors(hostname, pid)
            return await json_response(result=result)

        if not self.hosted_dashboard_client:
            # Hosted dashboard mode won't use local dashboard frontend.
            self.app.router.add_get("/", get_index)
            self.app.router.add_get("/favicon.ico", get_favicon)

            build_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "client/build")
            if not os.path.isdir(build_dir):
                raise ValueError(
                    "Dashboard build directory not found at '{}'."
                    "If installing from source, please follow the "
                    "additional steps required to build the dashboard: "
                    "cd python/ray/dashboard/client && npm ci && npm run build"
                    .format(build_dir))

            static_dir = os.path.join(build_dir, "static")
            self.app.router.add_static("/static", static_dir)

            speedscope_dir = os.path.join(build_dir, "speedscope-1.5.3")
            self.app.router.add_static("/speedscope", speedscope_dir)

        self.app.router.add_get("/api/ray_config", ray_config)
        self.app.router.add_get("/api/node_info", node_info)
        self.app.router.add_get("/api/raylet_info", raylet_info)
        self.app.router.add_get("/api/launch_profiling", launch_profiling)
        self.app.router.add_get("/api/check_profiling_status",
                                check_profiling_status)
        self.app.router.add_get("/api/get_profiling_info", get_profiling_info)
        self.app.router.add_get("/api/kill_actor", kill_actor)
        self.app.router.add_get("/api/logs", logs)
        self.app.router.add_get("/api/errors", errors)

        self.app.router.add_get("/{_}", get_forbidden)

    def log_dashboard_url(self):
        url = ray.services.get_webui_url_from_redis(self.redis_client)
        if url is None:
            raise ValueError("WebUI URL is not present in Redis.")
        with open(os.path.join(self.temp_dir, "dashboard_url"), "w") as f:
            f.write(url)
        logger.info("Dashboard running on {}".format(url))

    def run(self):
        self.log_dashboard_url()
        self.dashboard_controller.start_collecting_metrics()
        if self.hosted_dashboard_client:
            self.dashboard_client.start_exporting_metrics()
        aiohttp.web.run_app(self.app, host=self.host, port=self.port)


class NodeStats(threading.Thread):
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._node_stats = {}
        self._addr_to_owner_addr = {}
        self._addr_to_actor_id = {}
        self._addr_to_extra_info_dict = {}
        self._node_stats_lock = threading.Lock()

        self._default_info = {
            "actorId": "",
            "children": {},
            "currentTaskFuncDesc": [],
            "ipAddress": "",
            "isDirectCall": False,
            "jobId": "",
            "numExecutedTasks": 0,
            "numLocalObjects": 0,
            "numObjectIdsInScope": 0,
            "port": 0,
            "state": 0,
            "taskQueueLength": 0,
            "usedObjectStoreMemory": 0,
            "usedResources": {},
        }

        # Mapping from IP address to PID to list of log lines
        self._logs = defaultdict(lambda: defaultdict(list))

        # Mapping from IP address to PID to list of error messages
        self._errors = defaultdict(lambda: defaultdict(list))

        ray.state.state._initialize_global_state(
            redis_address=redis_address, redis_password=redis_password)

        super().__init__()

    def _calculate_log_counts(self):
        return {
            ip: {
                pid: len(logs_for_pid)
                for pid, logs_for_pid in logs_for_ip.items()
            }
            for ip, logs_for_ip in self._logs.items()
        }

    def _calculate_error_counts(self):
        return {
            ip: {
                pid: len(errors_for_pid)
                for pid, errors_for_pid in errors_for_ip.items()
            }
            for ip, errors_for_ip in self._errors.items()
        }

    def _purge_outdated_stats(self):
        def current(then, now):
            if (now - then) > 5:
                return False

            return True

        now = to_unix_time(datetime.datetime.utcnow())
        self._node_stats = {
            k: v
            for k, v in self._node_stats.items() if current(v["now"], now)
        }

    def get_node_stats(self) -> Dict:
        with self._node_stats_lock:
            self._purge_outdated_stats()
            node_stats = sorted(
                (v for v in self._node_stats.values()),
                key=itemgetter("boot_time"))
            return {
                "clients": node_stats,
                "log_counts": self._calculate_log_counts(),
                "error_counts": self._calculate_error_counts(),
            }

    def get_actor_tree(self, workers_info_by_node, infeasible_tasks) -> Dict:
        now = time.time()
        # construct flattened actor tree
        flattened_tree = {"root": {"children": {}}}
        child_to_parent = {}
        with self._node_stats_lock:
            for addr, actor_id in self._addr_to_actor_id.items():
                flattened_tree[actor_id] = copy.deepcopy(self._default_info)
                flattened_tree[actor_id].update(
                    self._addr_to_extra_info_dict[addr])
                parent_id = self._addr_to_actor_id.get(
                    self._addr_to_owner_addr[addr], "root")
                child_to_parent[actor_id] = parent_id

            for node_id, workers_info in workers_info_by_node.items():
                for worker_info in workers_info:
                    if "coreWorkerStats" in worker_info:
                        core_worker_stats = worker_info["coreWorkerStats"]
                        addr = (core_worker_stats["ipAddress"],
                                str(core_worker_stats["port"]))
                        if addr in self._addr_to_actor_id:
                            actor_info = flattened_tree[self._addr_to_actor_id[
                                addr]]
                            format_reply_id(core_worker_stats)
                            actor_info.update(core_worker_stats)
                            actor_info["averageTaskExecutionSpeed"] = round(
                                actor_info["numExecutedTasks"] /
                                (now - actor_info["timestamp"] / 1000), 2)
                            actor_info["nodeId"] = node_id
                            actor_info["pid"] = worker_info["pid"]

            for infeasible_task in infeasible_tasks:
                actor_id = ray.utils.binary_to_hex(
                    b64decode(
                        infeasible_task["actorCreationTaskSpec"]["actorId"]))
                caller_addr = (infeasible_task["callerAddress"]["ipAddress"],
                               str(infeasible_task["callerAddress"]["port"]))
                caller_id = self._addr_to_actor_id.get(caller_addr, "root")
                child_to_parent[actor_id] = caller_id
                infeasible_task["state"] = -1
                format_reply_id(infeasible_tasks)
                flattened_tree[actor_id] = infeasible_task

        # construct actor tree
        actor_tree = flattened_tree
        for actor_id, parent_id in child_to_parent.items():
            actor_tree[parent_id]["children"][actor_id] = actor_tree[actor_id]
        return actor_tree["root"]["children"]

    def get_logs(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        logs = self._logs.get(ip, {})
        if pid:
            logs = {pid: logs.get(pid, [])}
        return logs

    def get_errors(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        errors = self._errors.get(ip, {})
        if pid:
            errors = {pid: errors.get(pid, [])}
        return errors

    def run(self):
        p = self.redis_client.pubsub(ignore_subscribe_messages=True)

        p.psubscribe(self.redis_key)
        logger.info("NodeStats: subscribed to {}".format(self.redis_key))

        log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
        p.subscribe(log_channel)
        logger.info("NodeStats: subscribed to {}".format(log_channel))

        error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        p.subscribe(error_channel)
        logger.info("NodeStats: subscribed to {}".format(error_channel))

        actor_channel = ray.gcs_utils.TablePubsub.Value("ACTOR_PUBSUB")
        p.subscribe(actor_channel)
        logger.info("NodeStats: subscribed to {}".format(actor_channel))

        current_actor_table = ray.actors()
        with self._node_stats_lock:
            for actor_data in current_actor_table.values():
                addr = (actor_data["Address"]["IPAddress"],
                        str(actor_data["Address"]["Port"]))
                owner_addr = (actor_data["OwnerAddress"]["IPAddress"],
                              str(actor_data["OwnerAddress"]["Port"]))
                self._addr_to_owner_addr[addr] = owner_addr
                self._addr_to_actor_id[addr] = actor_data["ActorID"]
                self._addr_to_extra_info_dict[addr] = {
                    "jobId": actor_data["JobID"],
                    "state": actor_data["State"],
                    "isDirectCall": actor_data["IsDirectCall"],
                    "timestamp": actor_data["Timestamp"]
                }

        for x in p.listen():
            try:
                with self._node_stats_lock:
                    channel = ray.utils.decode(x["channel"])
                    data = x["data"]
                    if channel == log_channel:
                        data = json.loads(ray.utils.decode(data))
                        ip = data["ip"]
                        pid = str(data["pid"])
                        self._logs[ip][pid].extend(data["lines"])
                    elif channel == str(error_channel):
                        gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                        error_data = ray.gcs_utils.ErrorTableData.FromString(
                            gcs_entry.entries[0])
                        message = error_data.error_message
                        message = re.sub(r"\x1b\[\d+m", "", message)
                        match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
                        if match:
                            pid = match.group(1)
                            ip = match.group(2)
                            self._errors[ip][pid].append({
                                "message": message,
                                "timestamp": error_data.timestamp,
                                "type": error_data.type
                            })
                    elif channel == str(actor_channel):
                        gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                        actor_data = ray.gcs_utils.ActorTableData.FromString(
                            gcs_entry.entries[0])
                        addr = (actor_data.address.ip_address,
                                str(actor_data.address.port))
                        owner_addr = (actor_data.owner_address.ip_address,
                                      str(actor_data.owner_address.port))
                        self._addr_to_owner_addr[addr] = owner_addr
                        self._addr_to_actor_id[addr] = ray.utils.binary_to_hex(
                            actor_data.actor_id)
                        self._addr_to_extra_info_dict[addr] = {
                            "jobId": ray.utils.binary_to_hex(
                                actor_data.job_id),
                            "state": actor_data.state,
                            "isDirectCall": actor_data.is_direct_call,
                            "timestamp": actor_data.timestamp
                        }
                    else:
                        data = json.loads(ray.utils.decode(data))
                        self._node_stats[data["hostname"]] = data

            except Exception:
                logger.exception(traceback.format_exc())
                continue


class RayletStats(threading.Thread):
    def __init__(self, redis_address, update_frequency, redis_password=None):
        self.nodes_lock = threading.Lock()
        self.nodes = []
        self.stubs = {}
        self.reporter_stubs = {}
        self.update_frequency = update_frequency
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)

        self._raylet_stats_lock = threading.Lock()
        self._raylet_stats = {}
        self._profiling_stats = {}

        self._update_nodes()

        super().__init__()

    def _update_nodes(self):
        with self.nodes_lock:
            self.nodes = ray.nodes()
            node_ids = [node["NodeID"] for node in self.nodes]

            # First remove node connections of disconnected nodes.
            for node_id in self.stubs.keys():
                if node_id not in node_ids:
                    stub = self.stubs.pop(node_id)
                    stub.close()
                    reporter_stub = self.reporter_stubs.pop(node_id)
                    reporter_stub.close()

            # Now add node connections of new nodes.
            for node in self.nodes:
                node_id = node["NodeID"]
                if node_id not in self.stubs:
                    node_ip = node["NodeManagerAddress"]
                    channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, node["NodeManagerPort"]))
                    stub = node_manager_pb2_grpc.NodeManagerServiceStub(
                        channel)
                    self.stubs[node_id] = stub
                    # Block wait until the reporter for the node starts.
                    while True:
                        reporter_port = self.redis_client.get(
                            "REPORTER_PORT:{}".format(node_ip))
                        if reporter_port:
                            break
                    reporter_channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, int(reporter_port)))
                    reporter_stub = reporter_pb2_grpc.ReporterServiceStub(
                        reporter_channel)
                    self.reporter_stubs[node_id] = reporter_stub

            assert len(self.stubs) == len(
                self.reporter_stubs), (self.stubs.keys(),
                                       self.reporter_stubs.keys())

    def get_raylet_stats(self) -> Dict:
        with self._raylet_stats_lock:
            return copy.deepcopy(self._raylet_stats)

    def launch_profiling(self, node_id, pid, duration):
        profiling_id = str(uuid.uuid4())

        def _callback(reply_future):
            reply = reply_future.result()
            with self._raylet_stats_lock:
                self._profiling_stats[profiling_id] = reply

        reporter_stub = self.reporter_stubs[node_id]
        reply_future = reporter_stub.GetProfilingStats.future(
            reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        reply_future.add_done_callback(_callback)
        return profiling_id

    def check_profiling_status(self, profiling_id):
        with self._raylet_stats_lock:
            is_present = profiling_id in self._profiling_stats
        if not is_present:
            return {"status": "pending"}

        reply = self._profiling_stats[profiling_id]
        if reply.stderr:
            return {"status": "error", "error": reply.stderr}
        else:
            return {"status": "finished"}

    def get_profiling_info(self, profiling_id):
        with self._raylet_stats_lock:
            profiling_stats = self._profiling_stats.get(profiling_id)
        assert profiling_stats, "profiling not finished"
        return json.loads(profiling_stats.profiling_stats)

    def kill_actor(self, actor_id, ip_address, port):
        channel = grpc.insecure_channel("{}:{}".format(ip_address, int(port)))
        stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

        def _callback(reply_future):
            _ = reply_future.result()

        reply_future = stub.KillActor.future(
            core_worker_pb2.KillActorRequest(
                intended_actor_id=ray.utils.hex_to_binary(actor_id)))
        reply_future.add_done_callback(_callback)
        return {}

    def run(self):
        counter = 0
        while True:
            time.sleep(self.update_frequency)
            replies = {}
            try:
                for node in self.nodes:
                    node_id = node["NodeID"]
                    stub = self.stubs[node_id]
                    reply = stub.GetNodeStats(
                        node_manager_pb2.GetNodeStatsRequest(), timeout=2)
                    reply_dict = MessageToDict(reply)
                    reply_dict["nodeId"] = node_id
                    replies[node["NodeManagerAddress"]] = reply_dict
                with self._raylet_stats_lock:
                    for address, reply_dict in replies.items():
                        self._raylet_stats[address] = reply_dict
            except Exception:
                logger.exception(traceback.format_exc())
            finally:
                counter += 1
                # From time to time, check if new nodes have joined the cluster
                # and update self.nodes
                if counter % 10:
                    self._update_nodes()
