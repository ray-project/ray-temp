from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from googleapiclient import discovery

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME
from ray.autoscaler.gcp.config import MAX_POLLS, POLL_INTERVAL
from ray.ray_constants import BOTO_MAX_RETRIES


def wait_for_compute_zone_operation(compute, project_name, operation, zone):
    """TODO: This seems unnecessary. Figure out if we can get rid of this"""
    print('Waiting for operation {} to finish...'.format(operation['name']))

    for _ in range(MAX_POLLS):
        result = compute.zoneOperations().get(
            project=project_name,
            operation=operation['name'],
            zone=zone
        ).execute()
        if 'error' in result:
            raise Exception(result['error'])

        if result['status'] == 'DONE':
            print("Done.")
            break

        time.sleep(POLL_INTERVAL)

    return result


class GCPNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        self.compute = discovery.build('compute', 'v1')

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

        # Cache of ip lookups. We assume IPs never change once assigned.
        self.internal_ip_cache = {}
        self.external_ip_cache = {}

    def nodes(self, tag_filters):
        if tag_filters:
            label_filter_expr = '(' + ' AND '.join([
                '(labels.{key} = {value})'.format(key=key, value=value)
                for key, value in tag_filters.items()
            ]) + ')'
        else:
            label_filter_expr = ''

        instance_state_filter_expr = '(' + ' OR '.join([
            '(status = {status})'.format(status=status)
            for status in {'PROVISIONING', 'STAGING', 'RUNNING'}
        ]) + ')'

        cluster_name_filter_expr = (
            '(labels.{key} = {value})'
            ''.format(key=TAG_RAY_CLUSTER_NAME, value=self.cluster_name))

        not_empty_filters = [
            f for f in [
                    label_filter_expr,
                    instance_state_filter_expr,
                    cluster_name_filter_expr,
            ] if f
        ]

        filter_expr = ' AND '.join(not_empty_filters)

        response = self.compute.instances().list(
            project=self.provider_config['project_id'],
            zone=self.provider_config['availability_zone'],
            filter=filter_expr,
        ).execute()

        instances = response.get('items', [])
        # Note: All the operations use 'name' as the unique instance identifier
        self.cached_nodes = {i['name']: i for i in instances}

        return [i['name'] for i in instances]

    def is_running(self, node_id):
        node = self._node(node_id)
        return node['status'] == 'RUNNING'

    def is_terminated(self, node_id):
        node = self._node(node_id)
        return node['status'] not in {'PROVISIONING', 'STAGING', 'RUNNING'}

    def node_tags(self, node_id):
        node = self._node(node_id)
        labels = node.get('labels', {})
        return labels

    def set_node_tags(self, node_id, tags):
        labels = tags
        project_id = self.provider_config['project_id']
        availability_zone = self.provider_config['availability_zone']

        node = self._node(node_id)
        operation = self.compute.instances().setLabels(
            project=project_id,
            zone=availability_zone,
            instance=node_id,
            body={
                'labels': dict(node['labels'], **labels),
                'labelFingerprint': node['labelFingerprint']
            }
        ).execute()

        result = wait_for_compute_zone_operation(
            self.compute, project_id, operation, availability_zone)

        return result

    def external_ip(self, node_id):
        if node_id in self.external_ip_cache:
            return self.external_ip_cache[node_id]
        node = self._node(node_id)
        # TODO: Isn't there better and more reliable way to do this?
        ip = (node
              .get('networkInterfaces', [{}])[0]
              .get('accessConfigs', [{}])[0]
              .get('natIP', None))
        if ip:
            self.external_ip_cache[node_id] = ip
        return ip

    def internal_ip(self, node_id):
        if node_id in self.internal_ip_cache:
            return self.internal_ip_cache[node_id]
        node = self._node(node_id)
        ip = node.get('networkInterfaces', [{}])[0].get('networkIP')
        if ip:
            self.internal_ip_cache[node_id] = ip
        return ip

    def create_node(self, base_config, tags, count):
        # NOTE: gcp uses 'labels' instead of aws 'tags'
        # https://cloud.google.com/compute/docs/instances/create-start-instance#startinginstancwithimage
        labels = tags
        project_id = self.provider_config['project_id']
        availability_zone = self.provider_config['availability_zone']

        config = base_config.copy()
        config['name'] = labels[TAG_RAY_NODE_NAME]
        config['machineType'] = (
            'zones/{zone}/machineTypes/{machine_type}'
            ''.format(zone=availability_zone,
                      machine_type=base_config['machineType']))

        config['labels'] = dict(
            config.get('labels', {}),
            **labels,
            **{TAG_RAY_CLUSTER_NAME: self.cluster_name})

        if count != 1:
            print("WARNING: Requested to create {} nodes. GCPNodeProvider"
                  " currently only support creating one node at a time. This"
                  " possible discrepancy should already be handled in the"
                  " autoscaling loop.".format(count))

        operation = self.compute.instances().insert(
            project=project_id,
            zone=availability_zone,
            body=config
        ).execute()

        result = wait_for_compute_zone_operation(
            self.compute, project_id, operation, availability_zone)

        return result

    def terminate_node(self, node_id):
        project_id = self.provider_config['project_id']
        availability_zone = self.provider_config['availability_zone']

        operation = self.compute.instances().delete(
            project=project_id,
            zone=availability_zone,
            instance=node_id,
        ).execute()

        result = wait_for_compute_zone_operation(
            self.compute, project_id, operation, availability_zone)

        return result

    def _node(self, node_id):
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        instance = self.compute.instances().get(
            project=self.provider_config['project_id'],
            zone=self.provider_config['availability_zone'],
            instance=node_id,
        ).execute()

        return instance
