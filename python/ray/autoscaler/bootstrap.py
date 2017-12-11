from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.autoscaler.autoscaler import hash_files, hash_launch_conf
from ray.autoscaler.node_provider import get_node_provider
from ray.autoscaler.tags import TAG_RAY_NODE_TYPE, TAG_RAY_WORKER_GROUP, \
    TAG_RAY_LAUNCH_CONFIG, TAG_RAY_APPLIED_CONFIG
from ray.autoscaler.updater import NodeUpdater


def bootstrap_cluster(config):
    assert config["provider"] == "aws", \
        "Unsupported provider {}".format(config["provider"])
    _bootstrap_aws_cluster(config)


def _bootstrap_aws_cluster(config):
    _aws_get_or_create_iam_role(config)
    _aws_get_or_create_key_pair(config)
    _aws_get_or_create_head_node(config)


def _aws_get_or_create_iam_role(config):
    assert "IamInstanceProfile" in config["head_node"]  # TODO auto-create


def _aws_get_or_create_key_pair(config):
    assert "KeyName" in config["head_node"]  # TODO auto-create
    assert "KeyName" in config["node"]  # TODO auto-create


def _aws_get_or_create_head_node(config):
    provider = get_node_provider(
        config["provider"], config["worker_group"], config["head_node"])
    head_node_tags = {
        TAG_RAY_NODE_TYPE: "Head",
        TAG_RAY_WORKER_GROUP: config["worker_group"],
    }
    nodes = provider.nodes(head_node_tags)
    if len(nodes) > 0:
        head_node = nodes[0]
    else:
        head_node = None

    launch_hash = hash_launch_conf(config["head_node"])
    if head_node is None or provider.node_tags(head_node).get(
            TAG_RAY_LAUNCH_CONFIG) != launch_hash:
        if head_node is not None:
            print("Terminating outdated head node {}".format(head_node))
            provider.terminate_node(head_node)
        print("Launching new head node...")
        head_node_tags[TAG_RAY_LAUNCH_CONFIG] = launch_hash
        provider.create_node(
            "ray-head-{}".format(config["worker_group"]),
            head_node_tags, 1)

    nodes = provider.nodes(head_node_tags)
    assert len(nodes) == 1, "Failed to create head node."
    head_node = nodes[0]

    files_hash = hash_files(
        config["file_mounts"], config["head_init_commands"])
    if provider.node_tags(head_node).get(TAG_RAY_APPLIED_CONFIG) != files_hash:
        print("Updating files on head node...")
        updater = NodeUpdater(
            head_node,
            config["provider"],
            config["worker_group"],
            config["file_mounts"],
            config["head_init_commands"],
            files_hash,
            redirect_output = False)
        updater.start()
        updater.join()
    print(
        "Head node up-to-date, IP address is: {}".format(
            provider.external_ip(head_node)))
