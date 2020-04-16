import asyncio
from collections import defaultdict
from functools import wraps
import inspect

import ray
import ray.cloudpickle as pickle
from ray.serve.backend_config import BackendConfig
from ray.serve.constants import (ASYNC_CONCURRENCY, SERVE_ROUTER_NAME,
                                 SERVE_PROXY_NAME, SERVE_METRIC_MONITOR_NAME)
from ray.serve.exceptions import batch_annotation_not_found
from ray.serve.http_proxy import HTTPProxyActor
from ray.serve.metric import (MetricMonitor, start_metric_monitor_loop)
from ray.serve.backend_worker import create_backend_worker
from ray.serve.utils import get_random_letters, logger

import numpy as np


def async_retryable(cls):
    """Make all actor method invocations on the class retryable.

    Note: This will retry actor_handle.method_name.remote(), but it must
    be invoked in an async context.

    Usage:
        @ray.remote(max_reconstructions=10000)
        @retryable
        class A:
            pass
    """
    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):

        def decorate_with_retry(f):
            @wraps(f)
            async def retry_method(*args, **kwargs):
                while True:
                    result = await f(*args, **kwargs)
                    if isinstance(result, ray.exceptions.RayActorError):
                        logger.warning(
                            "Actor method '{}' failed, retrying after 100ms.".
                            format(name))
                        await asyncio.sleep(0.1)
                    else:
                        return result

            return retry_method

        method.__ray_invocation_decorator__ = decorate_with_retry
    return cls


@ray.remote
class ServeMaster:
    """Initialize and store all actor handles.

    Note:
        This actor is necessary because ray will destroy actors when the
        original actor handle goes out of scope (when driver exit). Therefore
        we need to initialize and store actor handles in a seperate actor.
    """

    def __init__(self, kv_store_connector, recovering=True):
        self.kv_store_client = kv_store_connector("serve_checkpoints")
        # path -> (endpoint, methods).
        self.routes = {}
        # backend -> (worker_creator, init_args, backend_config).
        self.backends = {}
        # backend -> replica_tags.
        self.replicas = defaultdict(list)
        self.replicas_to_start = defaultdict(list)
        self.replicas_to_stop = defaultdict(list)
        # endpoint -> traffic_dict
        self.traffic_policies = dict()
        # Dictionary of backend tag to dictionaries of replica tag to worker.
        self.workers = defaultdict(dict)
        self.router = None
        self.http_proxy = None
        self.metric_monitor = None

        if recovering:
            self._recover_from_checkpoint()

    def _checkpoint(self):
        # We need to checkpoint: tables, workers that we're removing, workers.
        checkpoint = pickle.dumps((self.routes, self.backends, self.replicas,
                                   self.traffic_policies))
        self.kv_store_client.put("checkpoint", checkpoint)

    def _recover_from_checkpoint(self):
        # 1) Check "base components" - they might not have been started yet, in
        # which case we need to start them.
        # 2) Check workers - they might not have been started yet, in which
        # case we need to start them.
        # 3) Delete the workers that we're removing (take a new checkpoint?)
        checkpoint = self.kv_store_client.get("checkpoint")
        (self.routes, self.backends, self.replicas,
         self.traffic_policies) = pickle.loads(checkpoint)
        # On startup, check that all of the expected workers actually exist,
        # create them if they don't.
        workers = pickle.loads(checkpoint)
        for backend, replicas in workers.items():
            for replica in replicas:
                self.workers[backend][replica] = ray.utils.get_actor(replica)

        # Then, for workers that are pending deletion, delete them.
        self.workers_to_kill = self.checkpoints.get("pending_deletion")
        self._kill_pending_workers()

        try:
            self.router = ray.utils.get_actor(SERVE_ROUTER_NAME)
        except ValueError:
            self.router = None
        try:
            self.http_proxy = ray.utils.get_actor(SERVE_PROXY_NAME)
        except ValueError:
            self.http_proxy = None
        try:
            self.metric_monitor = ray.utils.get_actor(SERVE_PROXY_NAME)
        except ValueError:
            self.metric_monitor = None

    def _kill_detached_actor(self, handle):
        worker = ray.worker.global_worker
        # Set no_reconstruction=True so the actor won't be reconstructed.
        worker.core_worker.kill_actor(handle._ray_actor_id, True)

    def _list_replicas(self, backend_tag):
        return self.replicas[backend_tag]

    def get_traffic_policy(self, endpoint):
        return self.traffic_policies[endpoint]

    def start_router(self, router_class, init_kwargs):
        assert self.router is None, "Router already started."
        self.router = async_retryable(router_class).options(
            name=SERVE_ROUTER_NAME,
            max_concurrency=ASYNC_CONCURRENCY,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
        ).remote(**init_kwargs)

    def get_router(self):
        assert self.router is not None, "Router not started yet."
        return [self.router]

    def start_http_proxy(self, host, port):
        """Start the HTTP proxy on the given host:port.

        On startup (or restart), the HTTP proxy will fetch its config via
        get_http_proxy_config.
        """
        assert self.http_proxy is None, "HTTP proxy already started."
        assert self.router is not None, (
            "Router must be started before HTTP proxy.")
        self.http_proxy = async_retryable(HTTPProxyActor).options(
            name=SERVE_PROXY_NAME,
            max_concurrency=ASYNC_CONCURRENCY,
            max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
        ).remote(host, port)

    async def get_http_proxy_config(self):
        return self.routes, self.get_router()

    def get_http_proxy(self):
        assert self.http_proxy is not None, "HTTP proxy not started yet."
        return [self.http_proxy]

    def start_metric_monitor(self, gc_window_seconds):
        assert self.metric_monitor is None, "Metric monitor already started."
        self.metric_monitor = MetricMonitor.options(
            name=SERVE_METRIC_MONITOR_NAME).remote(gc_window_seconds)
        # TODO(edoakes): this should be an actor method, not a separate task.
        start_metric_monitor_loop.remote(self.metric_monitor)
        self.metric_monitor.add_target.remote(self.router)

    def get_metric_monitor(self):
        assert self.metric_monitor is not None, (
            "Metric monitor not started yet.")
        return [self.metric_monitor]

    async def get_backend_worker_config(self):
        return self.get_router()

    def _start_backend_worker(self, backend_tag, replica_tag):
        logger.debug("Starting worker '{}' for backend '{}'.".format(
            replica_tag, backend_tag))
        worker_creator, init_args, config_dict = self.backends[backend_tag]
        # TODO(edoakes): just store the BackendConfig in self.backends.
        backend_config = BackendConfig(**config_dict)
        init_args = [backend_tag, replica_tag, init_args]
        kwargs = backend_config.get_actor_creation_args(init_args)
        kwargs["name"] = replica_tag
        kwargs[
            "max_reconstructions"] = ray.ray_constants.INFINITE_RECONSTRUCTION

        return ray.remote(worker_creator)._remote(**kwargs)

    async def _start_pending_replicas(self):
        # Note: they may already be created if we failed during this operation.
        for backend_tag, replicas_to_create in self.replicas_to_start.items():
            for replica_tag in replicas_to_create:
                try:
                    worker_handle = ray.util.get_actor(replica_tag)
                except ValueError:
                    worker_handle = self._start_backend_worker(
                        backend_tag, replica_tag)

                self.replicas[backend_tag].append(replica_tag)
                self.workers[backend_tag][replica_tag] = worker_handle

                # Wait for the worker to start up.
                await worker_handle.ready.remote()

                # Register the worker with the router.
                [router] = self.get_router()
                await router.add_new_worker.remote(backend_tag, worker_handle)

                # Register the worker with the metric monitor.
                self.get_metric_monitor()[0].add_target.remote(worker_handle)

        self.replicas_to_start.clear()

    async def _stop_pending_replicas(self):
        # Note: they may already be deleted if we failed during this operation.
        for backend_tag, replicas_to_stop in self.replicas_to_stop.items():
            for replica_tag in replicas_to_stop:
                try:
                    worker_handle = ray.util.get_actor(replica_tag)
                    # Remove the replica from metric monitor.
                    [monitor] = self.get_metric_monitor()
                    await monitor.remove_target.remote(worker_handle)

                    # Remove the replica from router.
                    # This will also destroy the actor handle.
                    [router] = self.get_router()
                    await router.remove_worker.remote(backend_tag,
                                                      worker_handle)
                except ValueError:
                    pass

        self.replicas_to_stop.clear()

    async def scale_replicas(self, backend_tag, num_replicas):
        """Scale the given backend to the number of replicas.

        This requires the master actor to be an async actor because we wait
        synchronously for backends to start up and they may make calls into
        the master actor while initializing (e.g., by calling get_handle()).
        """
        logger.debug("Scaling backend '{}' to {} replicas".format(
            backend_tag, num_replicas))
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert num_replicas >= 0, ("Number of replicas must be"
                                   " greater than or equal to 0.")

        current_num_replicas = len(self.replicas[backend_tag])
        delta_num_replicas = num_replicas - current_num_replicas

        if delta_num_replicas > 0:
            logger.debug("Adding {} replicas".format(delta_num_replicas))
            for _ in range(delta_num_replicas):
                replica_tag = "{}#{}".format(backend_tag, get_random_letters())
                self.replicas_to_start[backend_tag].append(replica_tag)
            # XXX
            #asyncio.get_event_loop().create_task(
            #self._start_pending_replicas())
            await self._start_pending_replicas()

        elif delta_num_replicas < 0:
            logger.debug("Removing {} replicas".format(-delta_num_replicas))
            assert len(self.replicas[backend_tag]) >= delta_num_replicas
            for _ in range(-delta_num_replicas):
                replica_tag = self.replicas[backend_tag].pop()
                if len(self.replicas[backend_tag]) == 0:
                    del self.replicas[backend_tag]
                del self.workers[backend_tag][replica_tag]
                if len(self.workers[backend_tag]) == 0:
                    del self.workers[backend_tag]

                self.replicas_to_stop[backend_tag].append(replica_tag)
            # XXX
            #asyncio.get_event_loop().create_task(self._stop_pending_replicas())
            await self._stop_pending_replicas()

        self._checkpoint()

    def get_all_worker_handles(self):
        return self.workers

    def get_all_endpoints(self):
        return [endpoint for endpoint, methods in self.routes.values()]

    async def split_traffic(self, endpoint_name, traffic_policy_dictionary):
        assert endpoint_name in self.get_all_endpoints()

        assert isinstance(traffic_policy_dictionary,
                          dict), "Traffic policy must be dictionary"
        prob = 0
        for backend, weight in traffic_policy_dictionary.items():
            prob += weight
            assert (backend in self.backends
                    ), "backend {} is not registered".format(backend)
        assert np.isclose(
            prob, 1, atol=0.02
        ), "weights must sum to 1, currently it sums to {}".format(prob)

        self.traffic_policies[endpoint_name] = traffic_policy_dictionary

        [router] = self.get_router()
        await router.set_traffic.remote(endpoint_name,
                                        traffic_policy_dictionary)

    async def create_endpoint(self, route, endpoint, methods):
        logger.debug(
            "Registering route {} to endpoint {} with methods {}.".format(
                route, endpoint, methods))
        # TODO(edoakes): reject existing routes.
        self.routes[route] = (endpoint, methods)

        [http_proxy] = self.get_http_proxy()
        await http_proxy.set_route_table.remote(self.routes)

    async def create_backend(self, backend_tag, backend_config, func_or_class,
                             actor_init_args):
        backend_config_dict = dict(backend_config)
        backend_worker = create_backend_worker(func_or_class)

        # Save creator that starts replicas, the arguments to be passed in,
        # and the configuration for the backends.
        self.backends[backend_tag] = (backend_worker, actor_init_args,
                                      backend_config_dict)

        # Set the backend config inside the router
        # (particularly for max-batch-size).
        [router] = self.get_router()
        await router.set_backend_config.remote(backend_tag,
                                               backend_config_dict)

        await self.scale_replicas(backend_tag,
                                  backend_config_dict["num_replicas"])

    async def set_backend_config(self, backend_tag, backend_config):
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        assert isinstance(backend_config,
                          BackendConfig), ("backend_config must be"
                                           " of instance BackendConfig")
        backend_config_dict = dict(backend_config)
        backend_worker, init_args, old_backend_config_dict = self.backends[
            backend_tag]

        if (not old_backend_config_dict["has_accept_batch_annotation"]
                and backend_config.max_batch_size is not None):
            raise batch_annotation_not_found

        self.backends[backend_tag] = (backend_worker, init_args,
                                      backend_config_dict)

        # Inform the router about change in configuration
        # (particularly for setting max_batch_size).
        [router] = self.get_router()
        await router.set_backend_config.remote(backend_tag,
                                               backend_config_dict)

        # Restart replicas if there is a change in the backend config related
        # to restart_configs.
        need_to_restart_replicas = any(
            old_backend_config_dict[k] != backend_config_dict[k]
            for k in BackendConfig.restart_on_change_fields)
        if need_to_restart_replicas:
            # Kill all the replicas for restarting with new configurations.
            await self.scale_replicas(backend_tag, 0)

        # Scale the replicas with the new configuration.
        await self.scale_replicas(backend_tag,
                                  backend_config_dict["num_replicas"])

    def get_backend_config(self, backend_tag):
        assert (backend_tag in self.backends
                ), "Backend {} is not registered.".format(backend_tag)
        return BackendConfig(**self.backends[backend_tag][2])
