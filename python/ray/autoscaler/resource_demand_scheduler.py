import copy
import numpy as np
import logging
import collections
from typing import List, Dict, Tuple

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_INSTANCE_TYPE

logger = logging.getLogger(__name__)


class ResourceDemandScheduler:
    def __init__(self, provider: NodeProvider, instance_types: Dict[str, dict],
                 max_workers: int):
        self.provider = provider
        self.instance_types = instance_types
        self.max_workers = max_workers

    def get_instances_to_launch(self, nodes: List[str],
                                resource_demands: List[dict]):
        """Get a list of instance types that should be added to the cluster.

        This method:
            (1) calculates the resources present in the cluster.
            (2) calculates the unfulfilled resource bundles.
            (3) calculates which instances need to be launched to fulfill all
                the bundle requests, subject to max_worker constraints.
        """

        if resource_demands is None:
            logger.info("No resource demands")
            return []

        node_resources = []
        instance_type_counts = collections.defaultdict(int)
        for node_id in nodes:
            tags = self.provider.node_tags(node_id)
            if TAG_RAY_INSTANCE_TYPE in tags:
                instance_type = tags[TAG_RAY_INSTANCE_TYPE]
                if instance_type not in self.instance_types:
                    raise RuntimeError(
                        "Missing entry for instance_type {} in "
                        "available_instance_types config: {}".format(
                            instance_type, self.instance_types))
                node_resources.append(self.instance_types[instance_type])
                instance_type_counts[instance_type] += 1
        logger.info("Node resources: {}".format(node_resources))

        unfulfilled = get_bin_pack_residual(node_resources, resource_demands)
        logger.info("Unfulfilled resources: {}".format(node_resources))

        instances = get_instances_for(
            self.instance_types, instance_type_counts,
            self.max_workers - len(nodes), unfulfilled)
        logger.info("Instance requests: {}".format(node_resources))
        return instances


def get_instances_for(instance_types: Dict[str, dict],
                      existing_instances: Dict[str, int], max_to_add: int,
                      resources: List[dict]) -> List[Tuple[str, int]]:
    instances_to_add = collections.defaultdict(int)
    allocated_resources = []

    while resources and sum(instances_to_add.values()) < max_to_add:
        utilization_scores = []
        for instance_type in instance_types:
            if (existing_instances.get(instance_type, 0) +
                    instances_to_add.get(instance_type, 0) >=
                    instance_types[instance_type]["max_workers"]):
                continue
            node_resources = instance_types[instance_type]["resources"]
            score = _utilization_score(node_resources, resources)
            if score is not None:
                utilization_scores.append((score, instance_type))

        # Give up, no feasible node.
        if not utilization_scores:
            break

        utilization_scores = sorted(utilization_scores, reverse=True)
        best_instance_type = utilization_scores[0][1]
        instances_to_add[best_instance_type] += 1
        allocated_resources.append(
            instance_types[best_instance_type]["resources"])
        residual = get_bin_pack_residual(allocated_resources[-1:], resources)
        assert len(residual) < len(resources), (resources, residual)
        resources = residual
        print("RES", best_instance_type, residual, utilization_scores)

    return list(instances_to_add.items())


def _utilization_score(node_resources, resources):
    remaining = copy.deepcopy(node_resources)

    fittable = []
    for r in resources:
        if _fits(remaining, r):
            fittable.append(r)
            _inplace_subtract(remaining, r)
    if not fittable:
        return None
    
    util_by_resources = []
    for k, v in node_resources.items():
        util = (v - remaining[k]) / v
        util_by_resources.append(v * (util ** 3))

    # Prioritize using all resources first, then prioritize overall balance
    # of multiple resources.
    return (min(util_by_resources), np.mean(util_by_resources))


def get_bin_pack_residual(node_resources: List[dict],
                          resource_demands: List[dict]):
    """Return a subset of resource_demands that cannot fit in the cluster.

    TODO(ekl): this currently does not guarantee the resources will be packed
    correctly by the Ray scheduler. This is only possible once the Ray backend
    supports a placement groups API.

    Args:
        node_resources (List[dict]): List of resources per node.
        resource_demands (List[dict]): List of resource bundles that need to
            be bin packed onto the nodes.

    Returns:
        List[dict] the residual list resources that do not fit.
    """

    unfulfilled = []

    # A most naive bin packing algorithm.
    nodes = copy.deepcopy(node_resources)
    for demand in resource_demands:
        found = False
        for node in nodes:
            if _fits(node, demand):
                _inplace_subtract(node, demand)
                print("Subtract", node, demand)
                found = True
                break
        if not found:
            unfulfilled.append(demand)

    return unfulfilled


def _fits(node, resources):
    for k, v in resources.items():
        if v > node.get(k, 0.0):
            return False
    return True


def _inplace_subtract(node, resources):
    for k, v in resources.items():
        assert k in node, (k, node)
        node[k] -= v
        assert node[k] >= 0.0, (node, k, v)
