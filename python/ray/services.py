import ray


# TODO(ekl) deprecate and move this to ray.util
def get_node_ip_address(address="8.8.8.8:53"):
    return ray.util.get_node_ip_address(address)
