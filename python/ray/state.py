from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import json
import logging
import sys
import time

import ray
from ray.function_manager import FunctionDescriptor

from ray.ray_constants import ID_SIZE
from ray import (
    gcs_utils,
    services,
)
from ray.utils import (decode, binary_to_object_id, binary_to_hex,
                       hex_to_binary)

logger = logging.getLogger(__name__)


def _parse_client_table(redis_client):
    """Read the client table.

    Args:
        redis_client: A client to the primary Redis shard.

    Returns:
        A list of information about the nodes in the cluster.
    """
    NIL_CLIENT_ID = ray.ObjectID.nil().binary()
    message = redis_client.execute_command(
        "RAY.TABLE_LOOKUP", gcs_utils.TablePrefix.Value('CLIENT'), "",
        NIL_CLIENT_ID)

    # Handle the case where no clients are returned. This should only
    # occur potentially immediately after the cluster is started.
    if message is None:
        return []

    node_info = {}
    gcs_entry = gcs_utils.GcsEntry.FromString(message)

    ordered_client_ids = []

    # Since GCS entries are append-only, we override so that
    # only the latest entries are kept.
    for entry in gcs_entry.entries:
        client = gcs_utils.ClientTableData.FromString(entry)

        resources = {
            client.resources_total_label[i]: client.resources_total_capacity[i]
            for i in range(len(client.resources_total_label))
        }
        client_id = ray.utils.binary_to_hex(client.client_id)

        if client.entry_type == gcs_utils.ClientTableData.INSERTION:
            ordered_client_ids.append(client_id)
            node_info[client_id] = {
                "ClientID": client_id,
                "EntryType": client.entry_type,
                "NodeManagerAddress": client.node_manager_address,
                "NodeManagerPort": client.node_manager_port,
                "ObjectManagerPort": client.object_manager_port,
                "ObjectStoreSocketName": client.object_store_socket_name,
                "RayletSocketName": client.raylet_socket_name,
                "Resources": resources
            }

        # If this client is being updated, then it must
        # have previously been inserted, and
        # it cannot have previously been removed.
        else:
            assert client_id in node_info, "Client not found!"
            assert node_info[client_id]["EntryType"] != gcs_utils.ClientTableData.DELETION, (
                "Unexpected updation of deleted client.")
            res_map = node_info[client_id]["Resources"]
            if client.entry_type == gcs_utils.ClientTableData.RES_CREATEUPDATE:
                for res in resources:
                    res_map[res] = resources[res]
            elif client.entry_type == gcs_utils.ClientTableData.RES_DELETE:
                for res in resources:
                    res_map.pop(res, None)
            elif client.entry_type == gcs_utils.ClientTableData.DELETION:
                pass  # Do nothing with the resmap if client deletion
            else:
                raise RuntimeError("Unexpected EntryType {}".format(
                    client.entry_type))
            node_info[client_id]["Resources"] = res_map
            node_info[client_id]["EntryType"] = client.entry_type
    # NOTE: We return the list comprehension below instead of simply doing
    # 'list(node_info.values())' in order to have the nodes appear in the order
    # that they joined the cluster. Python dictionaries do not preserve
    # insertion order. We could use an OrderedDict, but then we'd have to be
    # sure to only insert a given node a single time (clients that die appear
    # twice in the GCS log).
    return [node_info[client_id] for client_id in ordered_client_ids]


class GlobalState(object):
    """A class used to interface with the Ray control state.

    # TODO(zongheng): In the future move this to use Ray's redis module in the
    # backend to cut down on # of request RPCs.

    Attributes:
        redis_client: The Redis client used to query the primary redis server.
        redis_clients: Redis clients for each of the Redis shards.
    """

    def __init__(self):
        """Create a GlobalState object."""
        # The redis server storing metadata, such as function table, client
        # table, log files, event logs, workers/actions info.
        self.redis_client = None
        # Clients for the redis shards, storing the object table & task table.
        self.redis_clients = None

    def _check_connected(self):
        """Check that the object has been initialized before it is used.

        Raises:
            Exception: An exception is raised if ray.init() has not been called
                yet.
        """
        if self.redis_client is None:
            raise Exception("The ray global state API cannot be used before "
                            "ray.init has been called.")

        if self.redis_clients is None:
            raise Exception("The ray global state API cannot be used before "
                            "ray.init has been called.")

    def disconnect(self):
        """Disconnect global state from GCS."""
        self.redis_client = None
        self.redis_clients = None

    def _initialize_global_state(self,
                                 redis_address,
                                 redis_password=None,
                                 timeout=20):
        """Initialize the GlobalState object by connecting to Redis.

        It's possible that certain keys in Redis may not have been fully
        populated yet. In this case, we will retry this method until they have
        been populated or we exceed a timeout.

        Args:
            redis_address: The Redis address to connect.
            redis_password: The password of the redis server.
        """
        self.redis_client = services.create_redis_client(
            redis_address, redis_password)
        start_time = time.time()

        num_redis_shards = None
        redis_shard_addresses = []

        while time.time() - start_time < timeout:
            # Attempt to get the number of Redis shards.
            num_redis_shards = self.redis_client.get("NumRedisShards")
            if num_redis_shards is None:
                print("Waiting longer for NumRedisShards to be populated.")
                time.sleep(1)
                continue
            num_redis_shards = int(num_redis_shards)
            if num_redis_shards < 1:
                raise Exception("Expected at least one Redis shard, found "
                                "{}.".format(num_redis_shards))

            # Attempt to get all of the Redis shards.
            redis_shard_addresses = self.redis_client.lrange(
                "RedisShards", start=0, end=-1)
            if len(redis_shard_addresses) != num_redis_shards:
                print("Waiting longer for RedisShards to be populated.")
                time.sleep(1)
                continue

            # If we got here then we successfully got all of the information.
            break

        # Check to see if we timed out.
        if time.time() - start_time >= timeout:
            raise Exception("Timed out while attempting to initialize the "
                            "global state. num_redis_shards = {}, "
                            "redis_shard_addresses = {}".format(
                                num_redis_shards, redis_shard_addresses))

        # Get the rest of the information.
        self.redis_clients = []
        for shard_address in redis_shard_addresses:
            self.redis_clients.append(
                services.create_redis_client(shard_address.decode(),
                                             redis_password))

    def _execute_command(self, key, *args):
        """Execute a Redis command on the appropriate Redis shard based on key.

        Args:
            key: The object ID or the task ID that the query is about.
            args: The command to run.

        Returns:
            The value returned by the Redis command.
        """
        client = self.redis_clients[key.redis_shard_hash() % len(
            self.redis_clients)]
        return client.execute_command(*args)

    def _keys(self, pattern):
        """Execute the KEYS command on all Redis shards.

        Args:
            pattern: The KEYS pattern to query.

        Returns:
            The concatenated list of results from all shards.
        """
        result = []
        for client in self.redis_clients:
            result.extend(list(client.scan_iter(match=pattern)))
        return result

    def _object_table(self, object_id):
        """Fetch and parse the object table information for a single object ID.

        Args:
            object_id: An object ID to get information about.

        Returns:
            A dictionary with information about the object ID in question.
        """
        # Allow the argument to be either an ObjectID or a hex string.
        if not isinstance(object_id, ray.ObjectID):
            object_id = ray.ObjectID(hex_to_binary(object_id))

        # Return information about a single object ID.
        message = self._execute_command(object_id, "RAY.TABLE_LOOKUP",
                                        gcs_utils.TablePrefix.Value('OBJECT'), "",
                                        object_id.binary())
        if message is None:
            return {}
        gcs_entry = gcs_utils.GcsEntry.FromString(message)

        assert len(gcs_entry.entries) > 0

        entry = gcs_utils.ObjectTableData.FromString(gcs_entry.entries[0])

        object_info = {
            "DataSize": entry.object_size,
            "Manager": entry.manager,
        }

        return object_info

    def object_table(self, object_id=None):
        """Fetch and parse the object table info for one or more object IDs.

        Args:
            object_id: An object ID to fetch information about. If this is
                None, then the entire object table is fetched.

        Returns:
            Information from the object table.
        """
        self._check_connected()
        if object_id is not None:
            # Return information about a single object ID.
            return self._object_table(object_id)
        else:
            # Return the entire object table.
            object_keys = self._keys(gcs_utils.TablePrefix_OBJECT_string +
                                     "*")
            object_ids_binary = {
                key[len(gcs_utils.TablePrefix_OBJECT_string):]
                for key in object_keys
            }

            results = {}
            for object_id_binary in object_ids_binary:
                results[binary_to_object_id(object_id_binary)] = (
                    self._object_table(binary_to_object_id(object_id_binary)))
            return results

    def _task_table(self, task_id):
        """Fetch and parse the task table information for a single task ID.

        Args:
            task_id: A task ID to get information about.

        Returns:
            A dictionary with information about the task ID in question.
        """
        assert isinstance(task_id, ray.TaskID)
        message = self._execute_command(
            task_id, "RAY.TABLE_LOOKUP",
            gcs_utils.TablePrefix.Value('RAYLET_TASK'), "", task_id.binary())
        if message is None:
            return {}
        gcs_entries = gcs_utils.GcsEntry.FromString(message)

        assert len(gcs_entries.entries) == 1
        print(task_id)
        print(len(gcs_entries.entries[0]))
        task_table_data = gcs_utils.TaskTableData.FromString(
            gcs_entries.entries[0])
        print(task_table_data.task)
        task_table_message = gcs_utils.Task.GetRootAsTask(
            task_table_data.task, 0)

        execution_spec = task_table_message.TaskExecutionSpec()
        task_spec = task_table_message.TaskSpecification()
        task = ray._raylet.Task.from_string(task_spec)
        function_descriptor_list = task.function_descriptor_list()
        function_descriptor = FunctionDescriptor.from_bytes_list(
            function_descriptor_list)

        task_spec_info = {
            "DriverID": task.driver_id().hex(),
            "TaskID": task.task_id().hex(),
            "ParentTaskID": task.parent_task_id().hex(),
            "ParentCounter": task.parent_counter(),
            "ActorID": (task.actor_id().hex()),
            "ActorCreationID": task.actor_creation_id().hex(),
            "ActorCreationDummyObjectID": (
                task.actor_creation_dummy_object_id().hex()),
            "ActorCounter": task.actor_counter(),
            "Args": task.arguments(),
            "ReturnObjectIDs": task.returns(),
            "RequiredResources": task.required_resources(),
            "FunctionID": function_descriptor.function_id.hex(),
            "FunctionHash": binary_to_hex(function_descriptor.function_hash),
            "ModuleName": function_descriptor.module_name,
            "ClassName": function_descriptor.class_name,
            "FunctionName": function_descriptor.function_name,
        }

        return {
            "ExecutionSpec": {
                "Dependencies": [
                    execution_spec.Dependencies(i)
                    for i in range(execution_spec.DependenciesLength())
                ],
                "LastTimestamp": execution_spec.LastTimestamp(),
                "NumForwards": execution_spec.NumForwards()
            },
            "TaskSpec": task_spec_info
        }

    def task_table(self, task_id=None):
        """Fetch and parse the task table information for one or more task IDs.

        Args:
            task_id: A hex string of the task ID to fetch information about. If
                this is None, then the task object table is fetched.

        Returns:
            Information from the task table.
        """
        self._check_connected()
        if task_id is not None:
            task_id = ray.TaskID(hex_to_binary(task_id))
            return self._task_table(task_id)
        else:
            task_table_keys = self._keys(
                gcs_utils.TablePrefix_RAYLET_TASK_string + "*")
            task_ids_binary = [
                key[len(gcs_utils.TablePrefix_RAYLET_TASK_string):]
                for key in task_table_keys
            ]
            print([ray.TaskID(id) for id in task_ids_binary])

            results = {}
            for task_id_binary in task_ids_binary:
                results[binary_to_hex(task_id_binary)] = self._task_table(
                    ray.TaskID(task_id_binary))
            return results

    def client_table(self):
        """Fetch and parse the Redis DB client table.

        Returns:
            Information about the Ray clients in the cluster.
        """
        self._check_connected()

        return _parse_client_table(self.redis_client)

    def _profile_table(self, batch_id):
        """Get the profile events for a given batch of profile events.

        Args:
            batch_id: An identifier for a batch of profile events.

        Returns:
            A list of the profile events for the specified batch.
        """
        # TODO(rkn): This method should support limiting the number of log
        # events and should also support returning a window of events.
        message = self._execute_command(batch_id, "RAY.TABLE_LOOKUP",
                                        gcs_utils.TablePrefix.Value('PROFILE'), "",
                                        batch_id.binary())

        if message is None:
            return []

        gcs_entries = gcs_utils.GcsEntry.FromString(message)

        profile_events = []
        for entry in gcs_entries.entries:
            profile_table_message = gcs_utils.ProfileTableData.FromString(
                entry)

            component_type = profile_table_message.component_type
            component_id = binary_to_hex(profile_table_message.component_id)
            node_ip_address = profile_table_message.node_ip_address

            for profile_event_message in profile_table_message.profile_events:
                profile_event = {
                    "event_type": profile_event_message.event_type,
                    "component_id": component_id,
                    "node_ip_address": node_ip_address,
                    "component_type": component_type,
                    "start_time": profile_event_message.start_time,
                    "end_time": profile_event_message.end_time,
                    "extra_data": json.loads(profile_event_message.extra_data),
                }

                profile_events.append(profile_event)

        return profile_events

    def profile_table(self):
        self._check_connected()
        profile_table_keys = self._keys(
            gcs_utils.TablePrefix_PROFILE_string + "*")
        batch_identifiers_binary = [
            key[len(gcs_utils.TablePrefix_PROFILE_string):]
            for key in profile_table_keys
        ]

        result = defaultdict(list)
        for batch_id in batch_identifiers_binary:
            profile_data = self._profile_table(binary_to_object_id(batch_id))
            # Note that if keys are being evicted from Redis, then it is
            # possible that the batch will be evicted before we get it.
            if len(profile_data) > 0:
                component_id = profile_data[0]["component_id"]
                result[component_id].extend(profile_data)

        return dict(result)

    def _seconds_to_microseconds(self, time_in_seconds):
        """A helper function for converting seconds to microseconds."""
        time_in_microseconds = 10**6 * time_in_seconds
        return time_in_microseconds

    # Colors are specified at
    # https://github.com/catapult-project/catapult/blob/master/tracing/tracing/base/color_scheme.html.  # noqa: E501
    _default_color_mapping = defaultdict(
        lambda: "generic_work", {
            "worker_idle": "cq_build_abandoned",
            "task": "rail_response",
            "task:deserialize_arguments": "rail_load",
            "task:execute": "rail_animation",
            "task:store_outputs": "rail_idle",
            "wait_for_function": "detailed_memory_dump",
            "ray.get": "good",
            "ray.put": "terrible",
            "ray.wait": "vsync_highlight_color",
            "submit_task": "background_memory_dump",
            "fetch_and_run_function": "detailed_memory_dump",
            "register_remote_function": "detailed_memory_dump",
        })

    # These colors are for use in Chrome tracing.
    _chrome_tracing_colors = [
        "thread_state_uninterruptible",
        "thread_state_iowait",
        "thread_state_running",
        "thread_state_runnable",
        "thread_state_sleeping",
        "thread_state_unknown",
        "background_memory_dump",
        "light_memory_dump",
        "detailed_memory_dump",
        "vsync_highlight_color",
        "generic_work",
        "good",
        "bad",
        "terrible",
        # "black",
        # "grey",
        # "white",
        "yellow",
        "olive",
        "rail_response",
        "rail_animation",
        "rail_idle",
        "rail_load",
        "startup",
        "heap_dump_stack_frame",
        "heap_dump_object_type",
        "heap_dump_child_node_arrow",
        "cq_build_running",
        "cq_build_passed",
        "cq_build_failed",
        "cq_build_abandoned",
        "cq_build_attempt_runnig",
        "cq_build_attempt_passed",
        "cq_build_attempt_failed",
    ]

    def chrome_tracing_dump(self, filename=None):
        """Return a list of profiling events that can viewed as a timeline.

        To view this information as a timeline, simply dump it as a json file
        by passing in "filename" or using using json.dump, and then load go to
        chrome://tracing in the Chrome web browser and load the dumped file.
        Make sure to enable "Flow events" in the "View Options" menu.

        Args:
            filename: If a filename is provided, the timeline is dumped to that
                file.

        Returns:
            If filename is not provided, this returns a list of profiling
                events. Each profile event is a dictionary.
        """
        # TODO(rkn): Support including the task specification data in the
        # timeline.
        # TODO(rkn): This should support viewing just a window of time or a
        # limited number of events.

        self._check_connected()

        profile_table = self.profile_table()
        all_events = []

        for component_id_hex, component_events in profile_table.items():
            # Only consider workers and drivers.
            component_type = component_events[0]["component_type"]
            if component_type not in ["worker", "driver"]:
                continue

            for event in component_events:
                new_event = {
                    # The category of the event.
                    "cat": event["event_type"],
                    # The string displayed on the event.
                    "name": event["event_type"],
                    # The identifier for the group of rows that the event
                    # appears in.
                    "pid": event["node_ip_address"],
                    # The identifier for the row that the event appears in.
                    "tid": event["component_type"] + ":" +
                    event["component_id"],
                    # The start time in microseconds.
                    "ts": self._seconds_to_microseconds(event["start_time"]),
                    # The duration in microseconds.
                    "dur": self._seconds_to_microseconds(event["end_time"] -
                                                         event["start_time"]),
                    # What is this?
                    "ph": "X",
                    # This is the name of the color to display the box in.
                    "cname": self._default_color_mapping[event["event_type"]],
                    # The extra user-defined data.
                    "args": event["extra_data"],
                }

                # Modify the json with the additional user-defined extra data.
                # This can be used to add fields or override existing fields.
                if "cname" in event["extra_data"]:
                    new_event["cname"] = event["extra_data"]["cname"]
                if "name" in event["extra_data"]:
                    new_event["name"] = event["extra_data"]["name"]

                all_events.append(new_event)

        if filename is not None:
            with open(filename, "w") as outfile:
                json.dump(all_events, outfile)
        else:
            return all_events

    def chrome_tracing_object_transfer_dump(self, filename=None):
        """Return a list of transfer events that can viewed as a timeline.

        To view this information as a timeline, simply dump it as a json file
        by passing in "filename" or using using json.dump, and then load go to
        chrome://tracing in the Chrome web browser and load the dumped file.
        Make sure to enable "Flow events" in the "View Options" menu.

        Args:
            filename: If a filename is provided, the timeline is dumped to that
                file.

        Returns:
            If filename is not provided, this returns a list of profiling
                events. Each profile event is a dictionary.
        """
        self._check_connected()

        client_id_to_address = {}
        for client_info in self.client_table():
            client_id_to_address[client_info["ClientID"]] = "{}:{}".format(
                client_info["NodeManagerAddress"],
                client_info["ObjectManagerPort"])

        all_events = []

        for key, items in self.profile_table().items():
            # Only consider object manager events.
            if items[0]["component_type"] != "object_manager":
                continue

            for event in items:
                if event["event_type"] == "transfer_send":
                    object_id, remote_client_id, _, _ = event["extra_data"]

                elif event["event_type"] == "transfer_receive":
                    object_id, remote_client_id, _, _ = event["extra_data"]

                elif event["event_type"] == "receive_pull_request":
                    object_id, remote_client_id = event["extra_data"]

                else:
                    assert False, "This should be unreachable."

                # Choose a color by reading the first couple of hex digits of
                # the object ID as an integer and turning that into a color.
                object_id_int = int(object_id[:2], 16)
                color = self._chrome_tracing_colors[object_id_int % len(
                    self._chrome_tracing_colors)]

                new_event = {
                    # The category of the event.
                    "cat": event["event_type"],
                    # The string displayed on the event.
                    "name": event["event_type"],
                    # The identifier for the group of rows that the event
                    # appears in.
                    "pid": client_id_to_address[key],
                    # The identifier for the row that the event appears in.
                    "tid": client_id_to_address[remote_client_id],
                    # The start time in microseconds.
                    "ts": self._seconds_to_microseconds(event["start_time"]),
                    # The duration in microseconds.
                    "dur": self._seconds_to_microseconds(event["end_time"] -
                                                         event["start_time"]),
                    # What is this?
                    "ph": "X",
                    # This is the name of the color to display the box in.
                    "cname": color,
                    # The extra user-defined data.
                    "args": event["extra_data"],
                }
                all_events.append(new_event)

                # Add another box with a color indicating whether it was a send
                # or a receive event.
                if event["event_type"] == "transfer_send":
                    additional_event = new_event.copy()
                    additional_event["cname"] = "black"
                    all_events.append(additional_event)
                elif event["event_type"] == "transfer_receive":
                    additional_event = new_event.copy()
                    additional_event["cname"] = "grey"
                    all_events.append(additional_event)
                else:
                    pass

        if filename is not None:
            with open(filename, "w") as outfile:
                json.dump(all_events, outfile)
        else:
            return all_events

    def workers(self):
        """Get a dictionary mapping worker ID to worker information."""
        self._check_connected()

        worker_keys = self.redis_client.keys("Worker*")
        workers_data = {}

        for worker_key in worker_keys:
            worker_info = self.redis_client.hgetall(worker_key)
            worker_id = binary_to_hex(worker_key[len("Workers:"):])

            workers_data[worker_id] = {
                "node_ip_address": decode(worker_info[b"node_ip_address"]),
                "plasma_store_socket": decode(
                    worker_info[b"plasma_store_socket"])
            }
            if b"stderr_file" in worker_info:
                workers_data[worker_id]["stderr_file"] = decode(
                    worker_info[b"stderr_file"])
            if b"stdout_file" in worker_info:
                workers_data[worker_id]["stdout_file"] = decode(
                    worker_info[b"stdout_file"])
        return workers_data

    def _job_length(self):
        event_log_sets = self.redis_client.keys("event_log*")
        overall_smallest = sys.maxsize
        overall_largest = 0
        num_tasks = 0
        for event_log_set in event_log_sets:
            fwd_range = self.redis_client.zrange(
                event_log_set, start=0, end=0, withscores=True)
            overall_smallest = min(overall_smallest, fwd_range[0][1])

            rev_range = self.redis_client.zrevrange(
                event_log_set, start=0, end=0, withscores=True)
            overall_largest = max(overall_largest, rev_range[0][1])

            num_tasks += self.redis_client.zcount(
                event_log_set, min=0, max=time.time())
        if num_tasks == 0:
            return 0, 0, 0
        return overall_smallest, overall_largest, num_tasks

    def cluster_resources(self):
        """Get the current total cluster resources.

        Note that this information can grow stale as nodes are added to or
        removed from the cluster.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        self._check_connected()

        resources = defaultdict(int)
        clients = self.client_table()
        for client in clients:
            # Only count resources from latest entries of live clients.
            if client["EntryType"] != gcs_utils.ClientTableData.DELETION:
                for key, value in client["Resources"].items():
                    resources[key] += value
        return dict(resources)

    def _live_client_ids(self):
        """Returns a set of client IDs corresponding to clients still alive."""
        return {
            client["ClientID"]
            for client in self.client_table()
            if (client["EntryType"] != gcs_utils.ClientTableData.DELETION)
        }

    def available_resources(self):
        """Get the current available cluster resources.

        This is different from `cluster_resources` in that this will return
        idle (available) resources rather than total resources.

        Note that this information can grow stale as tasks start and finish.

        Returns:
            A dictionary mapping resource name to the total quantity of that
                resource in the cluster.
        """
        self._check_connected()

        available_resources_by_id = {}

        subscribe_clients = [
            redis_client.pubsub(ignore_subscribe_messages=True)
            for redis_client in self.redis_clients
        ]
        for subscribe_client in subscribe_clients:
            subscribe_client.subscribe(gcs_utils.XRAY_HEARTBEAT_CHANNEL)

        client_ids = self._live_client_ids()

        while set(available_resources_by_id.keys()) != client_ids:
            for subscribe_client in subscribe_clients:
                # Parse client message
                raw_message = subscribe_client.get_message()
                if (raw_message is None or raw_message["channel"] !=
                        gcs_utils.XRAY_HEARTBEAT_CHANNEL):
                    continue
                data = raw_message["data"]
                gcs_entries = gcs_utils.GcsEntry.FromString(data)
                heartbeat_data = gcs_entries.entries[0]
                message = gcs_utils.HeartbeatTableData.FromString(
                    heartbeat_data)
                # Calculate available resources for this client
                num_resources = len(message.resources_available_label)
                dynamic_resources = {}
                for i in range(num_resources):
                    resource_id = message.resources_available_label[i]
                    dynamic_resources[resource_id] = (
                        message.resources_available_capacity[i])

                # Update available resources for this client
                client_id = ray.utils.binary_to_hex(message.client_id)
                available_resources_by_id[client_id] = dynamic_resources

            # Update clients in cluster
            client_ids = self._live_client_ids()

            # Remove disconnected clients
            for client_id in available_resources_by_id.keys():
                if client_id not in client_ids:
                    del available_resources_by_id[client_id]

        # Calculate total available resources
        total_available_resources = defaultdict(int)
        for available_resources in available_resources_by_id.values():
            for resource_id, num_available in available_resources.items():
                total_available_resources[resource_id] += num_available

        # Close the pubsub clients to avoid leaking file descriptors.
        for subscribe_client in subscribe_clients:
            subscribe_client.close()

        return dict(total_available_resources)

    def _error_messages(self, driver_id):
        """Get the error messages for a specific driver.

        Args:
            driver_id: The ID of the driver to get the errors for.

        Returns:
            A list of the error messages for this driver.
        """
        assert isinstance(driver_id, ray.DriverID)
        message = self.redis_client.execute_command(
            "RAY.TABLE_LOOKUP", gcs_utils.TablePrefix.Value('ERROR_INFO'), "",
            driver_id.binary())

        # If there are no errors, return early.
        if message is None:
            return []

        gcs_entries = gcs_utils.GcsEntry.FromString(message)
        error_messages = []
        for entry in gcs_entries.entries:
            error_data = gcs_utils.ErrorTableData.FromString(entry)
            assert driver_id.binary() == error_data.driver_id
            error_message = {
                "type": error_data.type,
                "message": error_data.error_message,
                "timestamp": error_data.timestamp,
            }
            error_messages.append(error_message)
        return error_messages

    def error_messages(self, driver_id=None):
        """Get the error messages for all drivers or a specific driver.

        Args:
            driver_id: The specific driver to get the errors for. If this is
                None, then this method retrieves the errors for all drivers.

        Returns:
            A dictionary mapping driver ID to a list of the error messages for
                that driver.
        """
        self._check_connected()

        if driver_id is not None:
            assert isinstance(driver_id, ray.DriverID)
            return self._error_messages(driver_id)

        error_table_keys = self.redis_client.keys(
            gcs_utils.TablePrefix_ERROR_INFO_string + "*")
        driver_ids = [
            key[len(gcs_utils.TablePrefix_ERROR_INFO_string):]
            for key in error_table_keys
        ]

        return {
            binary_to_hex(driver_id): self._error_messages(
                ray.DriverID(driver_id))
            for driver_id in driver_ids
        }

    def actor_checkpoint_info(self, actor_id):
        """Get checkpoint info for the given actor id.
         Args:
            actor_id: Actor's ID.
         Returns:
            A dictionary with information about the actor's checkpoint IDs and
            their timestamps.
        """
        self._check_connected()
        message = self._execute_command(
            actor_id,
            "RAY.TABLE_LOOKUP",
            gcs_utils.TablePrefix.Value('ACTOR_CHECKPOINT_ID'),
            "",
            actor_id.binary(),
        )
        if message is None:
            return None
        gcs_entry = gcs_utils.GcsEntry.FromString(message)
        entry = gcs_utils.ActorCheckpointIdData.FromString(
            gcs_entry.entries[0])
        checkpoint_ids = [
            ray.ActorCheckpointID(checkpoint_id)
            for checkpoint_id in entry.checkpoint_ids
        ]
        return {
            "ActorID": ray.utils.binary_to_hex(entry.actor_id),
            "CheckpointIds": checkpoint_ids,
            "Timestamps": entry.timestamps,
        }


class DeprecatedGlobalState(object):
    """A class used to print errors when the old global state API is used."""

    def object_table(self, object_id=None):
        logger.warning(
            "ray.global_state.object_table() is deprecated and will be "
            "removed in a subsequent release. Use ray.objects() instead.")
        return ray.objects(object_id=object_id)

    def task_table(self, task_id=None):
        logger.warning(
            "ray.global_state.task_table() is deprecated and will be "
            "removed in a subsequent release. Use ray.tasks() instead.")
        return ray.tasks(task_id=task_id)

    def function_table(self, function_id=None):
        raise DeprecationWarning(
            "ray.global_state.function_table() is deprecated.")

    def client_table(self):
        logger.warning(
            "ray.global_state.client_table() is deprecated and will be "
            "removed in a subsequent release. Use ray.nodes() instead.")
        return ray.nodes()

    def profile_table(self):
        raise DeprecationWarning(
            "ray.global_state.profile_table() is deprecated.")

    def chrome_tracing_dump(self, filename=None):
        logger.warning(
            "ray.global_state.chrome_tracing_dump() is deprecated and will be "
            "removed in a subsequent release. Use ray.timeline() instead.")
        return ray.timeline(filename=filename)

    def chrome_tracing_object_transfer_dump(self, filename=None):
        logger.warning(
            "ray.global_state.chrome_tracing_object_transfer_dump() is "
            "deprecated and will be removed in a subsequent release. Use "
            "ray.object_transfer_timeline() instead.")
        return ray.object_transfer_timeline(filename=filename)

    def workers(self):
        raise DeprecationWarning("ray.global_state.workers() is deprecated.")

    def cluster_resources(self):
        logger.warning(
            "ray.global_state.cluster_resources() is deprecated and will be "
            "removed in a subsequent release. Use ray.cluster_resources() "
            "instead.")
        return ray.cluster_resources()

    def available_resources(self):
        logger.warning(
            "ray.global_state.available_resources() is deprecated and will be "
            "removed in a subsequent release. Use ray.available_resources() "
            "instead.")
        return ray.available_resources()

    def error_messages(self, driver_id=None):
        logger.warning(
            "ray.global_state.error_messages() is deprecated and will be "
            "removed in a subsequent release. Use ray.errors() "
            "instead.")
        return ray.errors(driver_id=driver_id)


state = GlobalState()
"""A global object used to access the cluster's global state."""

global_state = DeprecatedGlobalState()


def nodes():
    """Get a list of the nodes in the cluster.

    Returns:
        Information about the Ray clients in the cluster.
    """
    return state.client_table()


def tasks(task_id=None):
    """Fetch and parse the task table information for one or more task IDs.

    Args:
        task_id: A hex string of the task ID to fetch information about. If
            this is None, then the task object table is fetched.

    Returns:
        Information from the task table.
    """
    return state.task_table(task_id=task_id)


def objects(object_id=None):
    """Fetch and parse the object table info for one or more object IDs.

    Args:
        object_id: An object ID to fetch information about. If this is None,
            then the entire object table is fetched.

    Returns:
        Information from the object table.
    """
    return state.object_table(object_id=object_id)


def timeline(filename=None):
    """Return a list of profiling events that can viewed as a timeline.

    To view this information as a timeline, simply dump it as a json file by
    passing in "filename" or using using json.dump, and then load go to
    chrome://tracing in the Chrome web browser and load the dumped file.

    Args:
        filename: If a filename is provided, the timeline is dumped to that
            file.

    Returns:
        If filename is not provided, this returns a list of profiling events.
            Each profile event is a dictionary.
    """
    return state.chrome_tracing_dump(filename=filename)


def object_transfer_timeline(filename=None):
    """Return a list of transfer events that can viewed as a timeline.

    To view this information as a timeline, simply dump it as a json file by
    passing in "filename" or using using json.dump, and then load go to
    chrome://tracing in the Chrome web browser and load the dumped file. Make
    sure to enable "Flow events" in the "View Options" menu.

    Args:
        filename: If a filename is provided, the timeline is dumped to that
            file.

    Returns:
        If filename is not provided, this returns a list of profiling events.
            Each profile event is a dictionary.
    """
    return state.chrome_tracing_object_transfer_dump(filename=filename)


def cluster_resources():
    """Get the current total cluster resources.

    Note that this information can grow stale as nodes are added to or removed
    from the cluster.

    Returns:
        A dictionary mapping resource name to the total quantity of that
            resource in the cluster.
    """
    return state.cluster_resources()


def available_resources():
    """Get the current available cluster resources.

    This is different from `cluster_resources` in that this will return idle
    (available) resources rather than total resources.

    Note that this information can grow stale as tasks start and finish.

    Returns:
        A dictionary mapping resource name to the total quantity of that
            resource in the cluster.
    """
    return state.available_resources()


def errors(include_cluster_errors=True):
    """Get error messages from the cluster.

    Args:
        include_cluster_errors: True if we should include error messages for
            all drivers, and false if we should only include error messages for
            this specific driver.

    Returns:
        Error messages pushed from the cluster.
    """
    worker = ray.worker.global_worker
    error_messages = state.error_messages(driver_id=worker.task_driver_id)
    if include_cluster_errors:
        error_messages += state.error_messages(driver_id=ray.DriverID.nil())
    return error_messages
