from collections import defaultdict, deque
from typing import Any

import numpy as np
from dataclasses import dataclass

import ray
from ray.experimental.serve.utils import get_custom_object_id, logger


@dataclass
class Query:
    request_body: Any
    result_oid: ray.ObjectID

    @staticmethod
    def new(req: Any):
        return Query(request_body=req, result_oid=get_custom_object_id())


@dataclass
class WorkIntent:
    work_oid: ray.ObjectID

    @staticmethod
    def new():
        return WorkIntent(work_oid=get_custom_object_id())


class CentralizedQueues:
    """A "router" that routes request to available workers.

    It aceepts requests from `.produce` method and queues the request data.
    It also accepts work intention from workers via the `.consume` method.
    The traffic policy is used to match requests with their corresponding
    workers.

    Behavior:
        >>> # psuedo-code
        >>> queue = CentralizedQueues()
        >>> queue.produce('service-name', data)
        # nothing happens, request is queued.
        # returns result ObjectID, which will contains the final result
        >>> queue.consume('backend-1')
        # nothing happens, work intention is queued.
        # return work ObjectID, which will contains the future request payload
        >>> queue.link('service-name', 'backend-1')
        # here the producer is matched with consumer, request data is put into
        # work ObjectID, and the worker processs the request and store the
        # result into result ObjectID

    Traffic policy *splits* the traffic among different consumers. It behaves
    in a probablistic fashion:

    1. When all backends are ready to receive traffic, we will randomly choose
       a backend based on the weights assigned by traffic policy dictionary.

    2. When more than 1 but not all backends are ready, we will normalize the
       weights of the ready backends to 1 and choose a backend via sampling.

    3. When there is only 1 backend ready, we will directly use that backend.
    """

    def __init__(self):
        # service_name -> request queue
        self.queues = defaultdict(deque)

        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)

        # backend_name -> worker queue
        self.workers = defaultdict(deque)

    def produce(self, service, request_data):
        query = Query.new(request_data)
        self.queues[service].append(query)
        self.flush()
        return query.result_oid.binary()

    def consume(self, backend):
        intention = WorkIntent.new()
        self.workers[backend].append(intention)
        self.flush()
        return intention.work_oid.binary()

    def link(self, service, backend):
        logger.debug("Link %s with %s", service, backend)
        self.traffic[service][backend] = 1.0
        self.flush()

    def set_traffic(self, service, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", service,
                     traffic_dict)
        self.traffic[service] = traffic_dict
        self.flush()

    def flush(self):
        """In the default case, flush is calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        self._flush()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = set((backend
                                 for backend, queues in self.workers.items()
                                 if len(queues) > 0))
        return list(backends_in_policy.intersection(available_workers))

    def _flush(self):
        for service, queue in self.queues.items():
            ready_backends = self._get_available_backends(service)

            while len(queue) and len(ready_backends):
                # fast track, only one backend available
                if len(ready_backends) == 1:
                    backend = ready_backends[0]
                    request, work = (queue.popleft(),
                                     self.workers[backend].popleft())
                    ray.worker.global_worker.put_object(work.work_oid, request)

                # roll a dice among the rest
                else:
                    backend_weights = np.array([
                        self.traffic[service][backend_name]
                        for backend_name in ready_backends
                    ])
                    # normalize the weights to 1
                    backend_weights /= backend_weights.sum()
                    chosen_backend = np.random.choice(
                        ready_backends, p=backend_weights).squeeze()

                    request, work = (
                        queue.popleft(),
                        self.workers[chosen_backend].popleft(),
                    )
                    ray.worker.global_worker.put_object(work.work_oid, request)

                ready_backends = self._get_available_backends(service)


@ray.remote
class CentralizedQueuesActor(CentralizedQueues):
    self_handle = None

    def register_self_handle(self, handle_to_this_actor):
        self.self_handle = handle_to_this_actor

    def flush(self):
        if self.self_handle:
            self.self_handle._flush.remote()
        else:
            self._flush()
