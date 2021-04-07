from typing import Dict, Any, List, Optional, Tuple

import ray.cloudpickle as pickle
from ray.serve.common import BackendTag, EndpointTag, TrafficPolicy
from ray.serve.long_poll import LongPollNamespace
from ray.serve.kv_store import RayInternalKVStore
from ray.serve.long_poll import LongPollHost

CHECKPOINT_KEY = "serve-endpoint-state-checkpoint"


class EndpointState:
    """Manages all state for endpoints in the system.

    This class is *not* thread safe, so any state-modifying methods should be
    called with a lock held.
    """

    def __init__(self, kv_store: RayInternalKVStore,
                 long_poll_host: LongPollHost):
        self._kv_store = kv_store
        self._long_poll_host = long_poll_host
        self._routes: Dict[str, Tuple[EndpointTag, Any]] = dict()
        self._traffic_policies: Dict[EndpointTag, TrafficPolicy] = dict()
        self._python_methods: Dict[EndpointTag, List[str]] = dict()

        checkpoint = self._kv_store.get(CHECKPOINT_KEY)
        if checkpoint is not None:
            (self._routes, self._traffic_policies,
             self._python_methods) = pickle.loads(checkpoint)

        self._notify_route_table_changed()
        self._notify_traffic_policies_changed()

    def _checkpoint(self):
        self._kv_store.put(
            CHECKPOINT_KEY,
            pickle.dumps((self._routes, self._traffic_policies,
                          self._python_methods)))

    def _notify_route_table_changed(self):
        self._long_poll_host.notify_changed(LongPollNamespace.ROUTE_TABLE,
                                            self._routes)

    def _notify_traffic_policies_changed(
            self, filter_tag: Optional[EndpointTag] = None):
        for tag, policy in self._traffic_policies.items():
            if filter_tag is None or tag == filter_tag:
                self._long_poll_host.notify_changed(
                    (LongPollNamespace.TRAFFIC_POLICIES, tag),
                    policy,
                )

    def _get_route_for_endpoint(self, endpoint: EndpointTag) -> str:
        for route, (route_endpoint, _) in self._routes.items():
            if route_endpoint == endpoint:
                return route
        return None

    def update_endpoint(self,
                        endpoint: EndpointTag,
                        route: Optional[str],
                        methods: List[str],
                        traffic_policy: TrafficPolicy,
                        python_methods: Optional[List[str]] = None):
        """Create or update the given endpoint.

        This method is idempotent - if the endpoint already exists it will be
        updated to match the given parameters. Calling this twice with the same
        arguments is a no-op.
        """
        if route is None:
            route = endpoint

        if route in self._routes and self._routes[route][0] != endpoint:
            raise ValueError(f"route_prefix {route} is already registered.")

        if python_methods is None:
            python_methods = []

        existing_route = self._get_route_for_endpoint(endpoint)
        if existing_route is not None:
            if (self._routes[existing_route] == (endpoint, methods)
                    and self._traffic_policies[endpoint] == traffic_policy
                    and self._python_methods[endpoint] == python_methods):
                return
            else:
                del self._routes[existing_route]
                del self._traffic_policies[endpoint]
                del self._python_methods[endpoint]

        self._routes[route] = (endpoint, methods)
        self._traffic_policies[endpoint] = traffic_policy
        self._python_methods[endpoint] = python_methods

        self._checkpoint()
        self._notify_route_table_changed()
        self._notify_traffic_policies_changed(endpoint)

    def create_endpoint(self,
                        endpoint: EndpointTag,
                        route: Optional[str],
                        methods: List[str],
                        traffic_policy: TrafficPolicy,
                        python_methods: Optional[List[str]] = None):
        # If this is a headless endpoint with no route, key the endpoint
        # based on its name.
        # TODO(edoakes): we should probably just store routes and endpoints
        # separately.
        if route is None:
            route = endpoint

        if python_methods is None:
            python_methods = []

        err_prefix = "Cannot create endpoint."
        if route in self._routes:
            # Ensures this method is idempotent
            if self._routes[route] == (endpoint, methods):
                return
            else:
                raise ValueError("{} Route '{}' is already registered.".format(
                    err_prefix, route))

        if endpoint in self._traffic_policies:
            raise ValueError("{} Endpoint '{}' is already registered.".format(
                err_prefix, endpoint))

        self._routes[route] = (endpoint, methods)
        self._traffic_policies[endpoint] = traffic_policy
        self._python_methods[endpoint] = python_methods

        self._checkpoint()
        self._notify_route_table_changed()
        self._notify_traffic_policies_changed(endpoint)

    def set_traffic_policy(self, endpoint: EndpointTag,
                           traffic_policy: TrafficPolicy):
        if endpoint not in self._traffic_policies:
            raise ValueError("Attempted to assign traffic for an endpoint '{}'"
                             " that is not registered.".format(endpoint))

        self._traffic_policies[endpoint] = traffic_policy

        self._checkpoint()
        self._notify_traffic_policies_changed(endpoint)

    def shadow_traffic(self, endpoint: EndpointTag, backend: BackendTag,
                       proportion: float):
        if endpoint not in self._traffic_policies:
            raise ValueError("Attempted to shadow traffic from an "
                             "endpoint '{}' that is not registered."
                             .format(endpoint))

        self._traffic_policies[endpoint].set_shadow(backend, proportion)

        self._checkpoint()
        self._notify_traffic_policies_changed(endpoint)

    def get_endpoint_route(self, endpoint: EndpointTag) -> Optional[str]:
        for route, (route_endpoint, methods) in self._routes.items():
            if route_endpoint == endpoint:
                return route
        return None

    def get_endpoints(self) -> Dict[EndpointTag, Dict[str, Any]]:
        endpoints = {}
        for route, (endpoint, methods) in self._routes.items():
            if endpoint in self._traffic_policies:
                traffic_policy = self._traffic_policies[endpoint]
                traffic_dict = traffic_policy.traffic_dict
                shadow_dict = traffic_policy.shadow_dict
            else:
                traffic_dict = {}
                shadow_dict = {}

            endpoints[endpoint] = {
                "route": route if route.startswith("/") else None,
                "methods": methods,
                "traffic": traffic_dict,
                "shadows": shadow_dict,
                "python_methods": self._python_methods[endpoint],
            }
        return endpoints

    def delete_endpoint(self, endpoint: EndpointTag) -> None:
        # This method must be idempotent. We should validate that the
        # specified endpoint exists on the client.
        route = self._get_route_for_endpoint(endpoint)
        if route is None:
            return

        del self._routes[route]
        del self._traffic_policies[endpoint]
        del self._python_methods[endpoint]

        self._checkpoint()
        self._notify_route_table_changed()
