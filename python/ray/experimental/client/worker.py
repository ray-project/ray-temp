"""This file includes the Worker class which sits on the client side.
It implements the Ray API functions that are forwarded through grpc calls
to the server.
"""
import base64
import inspect
import json
import logging
import uuid
from collections import defaultdict
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Optional

from ray.util.inspect import is_cython
import grpc

import ray.cloudpickle as cloudpickle
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc
from ray.experimental.client.client_pickler import convert_to_arg
from ray.experimental.client.client_pickler import dumps_from_client
from ray.experimental.client.client_pickler import loads_from_server
from ray.experimental.client.common import ClientActorClass
from ray.experimental.client.common import ClientActorHandle
from ray.experimental.client.common import ClientObjectRef
from ray.experimental.client.common import ClientRemoteFunc
from ray.experimental.client.common import ClientStub
from ray.experimental.client.dataclient import DataClient

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self,
                 conn_str: str = "",
                 secure: bool = False,
                 metadata: List[Tuple[str, str]] = None):
        """Initializes the worker side grpc client.

        Args:
            secure: whether to use SSL secure channel or not.
            metadata: additional metadata passed in the grpc request headers.
        """
        self.metadata = metadata
        self.channel = None
        self._client_id = make_client_id()
        if secure:
            credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(conn_str, credentials)
        else:
            self.channel = grpc.insecure_channel(conn_str)
        self.server = ray_client_pb2_grpc.RayletDriverStub(self.channel)
        self.data_client = DataClient(self.channel, self._client_id)
        self.reference_count: Dict[bytes, int] = defaultdict(int)

    def get(self, vals, *, timeout: Optional[float] = None) -> Any:
        to_get = []
        single = False
        if isinstance(vals, list):
            to_get = vals
        elif isinstance(vals, ClientObjectRef):
            to_get = [vals]
            single = True
        else:
            raise Exception("Can't get something that's not a "
                            "list of IDs or just an ID: %s" % type(vals))
        if timeout is None:
            timeout = 0
        out = [self._get(x, timeout) for x in to_get]
        if single:
            out = out[0]
        return out

    def _get(self, ref: ClientObjectRef, timeout: float):
        req = ray_client_pb2.GetRequest(id=ref.id, timeout=timeout)
        try:
            data = self.data_client.GetObject(req)
        except grpc.RpcError as e:
            raise e.details()
        if not data.valid:
            err = cloudpickle.loads(data.error)
            logger.error(err)
            raise err
        return loads_from_server(data.data)

    def put(self, vals):
        to_put = []
        single = False
        if isinstance(vals, list):
            to_put = vals
        else:
            single = True
            to_put.append(vals)

        out = [self._put(x) for x in to_put]
        if single:
            out = out[0]
        return out

    def _put(self, val):
        if isinstance(val, ClientObjectRef):
            raise TypeError(
                "Calling 'put' on an ObjectRef is not allowed "
                "(similarly, returning an ObjectRef from a remote "
                "function is not allowed). If you really want to "
                "do this, you can wrap the ObjectRef in a list and "
                "call 'put' on it (or return it).")
        data = dumps_from_client(val, self._client_id)
        req = ray_client_pb2.PutRequest(data=data)
        resp = self.data_client.PutObject(req)
        return ClientObjectRef(resp.id)

    def wait(self,
             object_refs: List[ClientObjectRef],
             *,
             num_returns: int = 1,
             timeout: float = None,
             fetch_local: bool = True
             ) -> Tuple[List[ClientObjectRef], List[ClientObjectRef]]:
        if not isinstance(object_refs, list):
            raise TypeError("wait() expected a list of ClientObjectRef, "
                            f"got {type(object_refs)}")
        for ref in object_refs:
            if not isinstance(ref, ClientObjectRef):
                raise TypeError("wait() expected a list of ClientObjectRef, "
                                f"got list containing {type(ref)}")
        data = {
            "object_ids": [object_ref.id for object_ref in object_refs],
            "num_returns": num_returns,
            "timeout": timeout if timeout else -1,
            "client_id": self._client_id,
        }
        req = ray_client_pb2.WaitRequest(**data)
        resp = self.server.WaitObject(req, metadata=self.metadata)
        if not resp.valid:
            # TODO(ameer): improve error/exceptions messages.
            raise Exception("Client Wait request failed. Reference invalid?")
        client_ready_object_ids = [
            ClientObjectRef(ref) for ref in resp.ready_object_ids
        ]
        client_remaining_object_ids = [
            ClientObjectRef(ref) for ref in resp.remaining_object_ids
        ]

        return (client_ready_object_ids, client_remaining_object_ids)

    def remote(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # This is the case where the decorator is just @ray.remote.
            return remote_decorator(options=None)(args[0])
        error_string = ("The @ray.remote decorator must be applied either "
                        "with no arguments and no parentheses, for example "
                        "'@ray.remote', or it must be applied using some of "
                        "the arguments 'num_returns', 'num_cpus', 'num_gpus', "
                        "'memory', 'object_store_memory', 'resources', "
                        "'max_calls', or 'max_restarts', like "
                        "'@ray.remote(num_returns=2, "
                        "resources={\"CustomResource\": 1})'.")
        assert len(args) == 0 and len(kwargs) > 0, error_string
        return remote_decorator(options=kwargs)

    def call_remote(self, instance, *args, **kwargs) -> List[bytes]:
        task = instance._prepare_client_task()
        for arg in args:
            pb_arg = convert_to_arg(arg, self._client_id)
            task.args.append(pb_arg)
        for k, v in kwargs.items():
            task.kwargs[k].CopyFrom(convert_to_arg(v, self._client_id))
        task.client_id = self._client_id
        logger.debug("Scheduling %s" % task)
        try:
            ticket = self.server.Schedule(task, metadata=self.metadata)
        except grpc.RpcError as e:
            raise decode_exception(e.details)
        if not ticket.valid:
            raise cloudpickle.loads(ticket.error)
        return ticket.return_ids

    def call_release(self, id: bytes) -> None:
        self.reference_count[id] -= 1
        if self.reference_count[id] == 0:
            self._release_server(id)
            del self.reference_count[id]

    def _release_server(self, id: bytes) -> None:
        if self.data_client is not None:
            logger.debug(f"Releasing {id}")
            self.data_client.ReleaseObject(
                ray_client_pb2.ReleaseRequest(ids=[id]))

    def call_retain(self, id: bytes) -> None:
        logger.debug(f"Retaining {id}")
        self.reference_count[id] += 1

    def close(self):
        self.data_client.close(close_channel=True)
        self.server = None
        if self.channel:
            self.channel = None

    def terminate_actor(self, actor: ClientActorHandle,
                        no_restart: bool) -> None:
        if not isinstance(actor, ClientActorHandle):
            raise ValueError("ray.kill() only supported for actors. "
                             "Got: {}.".format(type(actor)))
        term_actor = ray_client_pb2.TerminateRequest.ActorTerminate()
        term_actor.id = actor.actor_ref.id
        term_actor.no_restart = no_restart
        try:
            term = ray_client_pb2.TerminateRequest(actor=term_actor)
            term.client_id = self._client_id
            self.server.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e.details())

    def terminate_task(self, obj: ClientObjectRef, force: bool,
                       recursive: bool) -> None:
        if not isinstance(obj, ClientObjectRef):
            raise TypeError(
                "ray.cancel() only supported for non-actor object refs. "
                f"Got: {type(obj)}.")
        term_object = ray_client_pb2.TerminateRequest.TaskObjectTerminate()
        term_object.id = obj.id
        term_object.force = force
        term_object.recursive = recursive
        try:
            term = ray_client_pb2.TerminateRequest(task_object=term_object)
            term.client_id = self._client_id
            self.server.Terminate(term)
        except grpc.RpcError as e:
            raise decode_exception(e.details())

    def get_cluster_info(self, type: ray_client_pb2.ClusterInfoType.TypeEnum):
        req = ray_client_pb2.ClusterInfoRequest()
        req.type = type
        resp = self.server.ClusterInfo(req)
        if resp.WhichOneof("response_type") == "resource_table":
            return resp.resource_table.table
        return json.loads(resp.json)

    def is_initialized(self) -> bool:
        if self.server is not None:
            return self.get_cluster_info(
                ray_client_pb2.ClusterInfoType.IS_INITIALIZED)
        return False


def remote_decorator(options: Optional[Dict[str, Any]]):
    def decorator(function_or_class) -> ClientStub:
        if (inspect.isfunction(function_or_class)
                or is_cython(function_or_class)):
            return ClientRemoteFunc(function_or_class, options=options)
        elif inspect.isclass(function_or_class):
            return ClientActorClass(function_or_class, options=options)
        else:
            raise TypeError("The @ray.remote decorator must be applied to "
                            "either a function or to a class.")
    return decorator


def make_client_id() -> str:
    id = uuid.uuid4()
    return id.hex


def decode_exception(data) -> Exception:
    data = base64.standard_b64decode(data)
    return loads_from_server(data)
