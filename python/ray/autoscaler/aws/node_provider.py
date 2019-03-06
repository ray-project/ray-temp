from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import threading
from collections import defaultdict

import boto3
from botocore.config import Config

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.ray_constants import BOTO_MAX_RETRIES
from ray.autoscaler.log_timer import LogTimer


def to_aws_format(tags):
    """Convert the Ray node name tag to the AWS-specific 'Name' tag."""

    if TAG_RAY_NODE_NAME in tags:
        tags["Name"] = tags[TAG_RAY_NODE_NAME]
        del tags[TAG_RAY_NODE_NAME]
    return tags


def from_aws_format(tags):
    """Convert the AWS-specific 'Name' tag to the Ray node name tag."""

    if "Name" in tags:
        tags[TAG_RAY_NODE_NAME] = tags["Name"]
        del tags["Name"]
    return tags


class AWSNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        config = Config(retries={"max_attempts": BOTO_MAX_RETRIES})
        self.ec2 = boto3.resource(
            "ec2", region_name=provider_config["region"], config=config)

        # Try availability zones round-robin, starting from random offset
        self.subnet_idx = random.randint(0, 100)

        self.tag_cache = {}  # Tags that we believe to actually be on EC2.
        self.tag_cache_pending = {}  # Tags that we will soon upload.
        self.tag_cache_lock = threading.Lock()
        self.tag_cache_update_event = threading.Event()
        self.tag_cache_kill_event = threading.Event()
        self.tag_update_thread = threading.Thread(
            target=self._node_tag_update_loop)
        self.tag_update_thread.start()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

    def _node_tag_update_loop(self):
        """ Update the AWS tags for a cluster periodically.

        The purpose of this loop is to avoid excessive EC2 calls when a large
        number of nodes are being launched simultaneously.
        """
        while True:
            self.tag_cache_update_event.wait()
            self.tag_cache_update_event.clear()

            batch_updates = defaultdict(list)

            with self.tag_cache_lock:
                for node_id, tags in self.tag_cache_pending.items():
                    for x in tags.items():
                        batch_updates[x].append(node_id)
                    self.tag_cache[node_id].update(tags)

                self.tag_cache_pending = {}

            for (k, v), node_ids in batch_updates.items():
                m = "Set tag {}={} on {}".format(k, v, node_ids)
                with LogTimer("AWSNodeProvider: {}".format(m)):
                    if k == TAG_RAY_NODE_NAME:
                        k = "Name"
                    self.ec2.meta.client.create_tags(
                        Resources=node_ids,
                        Tags=[{
                            "Key": k,
                            "Value": v
                        }],
                    )

            self.tag_cache_kill_event.wait(timeout=5)
            if self.tag_cache_kill_event.is_set():
                return

    def non_terminated_nodes(self, tag_filters):
        # Note that these filters are acceptable because they are set on
        #       node initialization, and so can never be sitting in the cache.
        tag_filters = to_aws_format(tag_filters)
        filters = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running"],
            },
            {
                "Name": "tag:{}".format(TAG_RAY_CLUSTER_NAME),
                "Values": [self.cluster_name],
            },
        ]
        for k, v in tag_filters.items():
            filters.append({
                "Name": "tag:{}".format(k),
                "Values": [v],
            })

        nodes = list(self.ec2.instances.filter(Filters=filters))
        # Populate the tag cache with initial information if necessary
        for node in nodes:
            if node.id in self.tag_cache:
                continue

            self.tag_cache[node.id] = from_aws_format(
                {x["Key"]: x["Value"]
                 for x in node.tags})

        self.cached_nodes = {node.id: node for node in nodes}
        return [node.id for node in nodes]

    def is_running(self, node_id):
        node = self._get_cached_node(node_id)
        return node.state["Name"] == "running"

    def is_terminated(self, node_id):
        node = self._get_cached_node(node_id)
        state = node.state["Name"]
        return state not in ["running", "pending"]

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            d1 = self.tag_cache[node_id]
            d2 = self.tag_cache_pending.get(node_id, {})
            return dict(d1, **d2)

    def external_ip(self, node_id):
        node = self._get_cached_node(node_id)

        if node.public_ip_address is None:
            node = self._get_node(node_id)

        return node.public_ip_address

    def internal_ip(self, node_id):
        node = self._get_cached_node(node_id)

        if node.private_ip_address is None:
            node = self._get_node(node_id)

        return node.private_ip_address

    def set_node_tags(self, node_id, tags):
        with self.tag_cache_lock:
            try:
                self.tag_cache_pending[node_id].update(tags)
            except KeyError:
                self.tag_cache_pending[node_id] = tags

            self.tag_cache_update_event.set()

    def create_nodes(self, node_config, tags, count):
        tags = to_aws_format(tags)
        conf = node_config.copy()
        tag_pairs = [{
            "Key": TAG_RAY_CLUSTER_NAME,
            "Value": self.cluster_name,
        }]
        for k, v in tags.items():
            tag_pairs.append({
                "Key": k,
                "Value": v,
            })
        tag_specs = [{
            "ResourceType": "instance",
            "Tags": tag_pairs,
        }]
        user_tag_specs = conf.get("TagSpecifications", [])
        # Allow users to add tags and override values of existing
        # tags with their own. This only applies to the resource type
        # "instance". All other resource types are appended to the list of
        # tag specs.
        for user_tag_spec in user_tag_specs:
            if user_tag_spec["ResourceType"] == "instance":
                for user_tag in user_tag_spec["Tags"]:
                    exists = False
                    for tag in tag_specs[0]["Tags"]:
                        if user_tag["Key"] == tag["Key"]:
                            exists = True
                            tag["Value"] = user_tag["Value"]
                            break
                    if not exists:
                        tag_specs[0]["Tags"] += [user_tag]
            else:
                tag_specs += [user_tag_spec]

        # SubnetIds is not a real config key: we must resolve to a
        # single SubnetId before invoking the AWS API.
        subnet_ids = conf.pop("SubnetIds")
        subnet_id = subnet_ids[self.subnet_idx % len(subnet_ids)]
        self.subnet_idx += 1
        conf.update({
            "MinCount": 1,
            "MaxCount": count,
            "SubnetId": subnet_id,
            "TagSpecifications": tag_specs
        })
        self.ec2.create_instances(**conf)

    def terminate_nodes(self, node_ids):
        self.ec2.meta.client.terminate_instances(InstanceIds=node_ids)

        for node_id in node_ids:
            self.tag_cache.pop(node_id, None)
            self.tag_cache_pending.pop(node_id, None)

    def terminate_node(self, node_id):
        self.terminate_nodes([node_id])

    def _get_nodes(self, node_ids):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        expected_nodes = set(node_ids)
        found_nodes = set(self.cached_nodes.keys())
        missing_nodes = expected_nodes - found_nodes

        if not missing_nodes:
            return [self.cached_nodes[node_id] for node_id in node_ids]

        fetched_nodes = list(
            self.ec2.instances.filter(InstanceIds=list(missing_nodes)))
        missing_nodes -= set(node.id for node in fetched_nodes)
        assert not missing_nodes, missing_nodes

        self.cached_nodes.update({node.id: node for node in fetched_nodes})

        result = [self.cached_nodes[node_id] for node_id in node_ids]

        return result

    def _get_node(self, node_id):
        return self._get_nodes([node_id])[0]

    def _get_cached_nodes(self, node_ids):
        nodes_by_id = {
            node_id: self.cached_nodes.get(node_id, None)
            for node_id in node_ids
        }

        non_cached_node_ids = [k for k, v in nodes_by_id.items() if v is None]

        if non_cached_node_ids:
            # Note: this fetches the nodes and saves them to self.cached_nodes
            self._get_nodes(non_cached_node_ids)

        result = [self.cached_nodes[node_id] for node_id in node_ids]

        return result

    def _get_cached_node(self, node_id):
        return self._get_cached_nodes([node_id])[0]

    def cleanup(self):
        self.tag_cache_update_event.set()
        self.tag_cache_kill_event.set()
