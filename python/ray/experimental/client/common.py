import ray.core.generated.ray_client_pb2 as ray_client_pb2
from ray.experimental.client import ray
from typing import Any
from typing import Dict
from ray import cloudpickle


class ClientBaseRef:
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return "%s(%s)" % (
            type(self).__name__,
            self.id.hex(),
        )

    def __eq__(self, other):
        return self.id == other.id

    def binary(self):
        return self.id


class ClientObjectRef(ClientBaseRef):
    pass


class ClientActorNameRef(ClientBaseRef):
    pass


class ClientRemoteFunc:
    """
    A stub created on the Ray Client to represent a remote
    function that can be exectued on the cluster. This class
    is allowed to be passed around between remote functions.

    Attributes:
        _func: The actual function to execute remotely
        _name: The original name of the function
        _ref: The ClientObjectRef of the pickled code of the function, _func
        _raylet_remote: The Raylet-side ray.remote_function.RemoteFunction
            for this object
    """

    def __init__(self, f):
        self._func = f
        self._name = f.__name__
        self.id = None

        # self._ref can be lazily instantiated. Rather than
        # eagerly creating function data objects in the server
        # we can put them just before we execute the function,
        # especially in cases where many @ray.remote functions exist in
        # a library and only a handful are ever executed by a user
        # of the library.
        self._ref = None
        self._raylet_remote = None

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote function cannot be called directly. "
                        "Use {self._name}.remote method instead")

    def remote(self, *args, **kwargs):
        return ray.call_remote(self, *args, **kwargs)

    def _get_ray_remote_impl(self):
        if self._raylet_remote is None:
            self._raylet_remote = ray.remote(self._func)
        return self._raylet_remote

    def __repr__(self):
        return "ClientRemoteFunc(%s, %s)" % (self._name, self._ref)

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        if self._ref is None:
            self._ref = ray.put(self._func)
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.FUNCTION
        task.name = self._name
        task.payload_id = self._ref.id
        return task


class ClientActorClass:
    """
    A stub created on the Ray Client to represent an actor class
    wrapped by ray.remote and can be executed on the cluster.

    Attributes:
        actor_cls: The actual class to execute remotely
        _name: The original name of the class
        _ref: The ClientObjectRef of the pickled `actor_cls`
        _raylet_remote: The Raylet-side ray.ActorClass for this object
    """

    def __init__(self, actor_cls):
        self.actor_cls = actor_cls
        self._name = actor_cls.__name__
        self._ref = None
        self._raylet_remote = None

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote actor cannot be instantiated directly. "
                        "Use {self._name}.remote() instead")

    def __getstate__(self) -> Dict:
        state = {
            "actor_cls": self.actor_cls,
            "_name": self._name,
            "_ref": self._ref,
        }
        return state

    def __setstate__(self, state: Dict) -> None:
        self.actor_cls = state["actor_cls"]
        self._name = state["_name"]
        self._ref = state["_ref"]

    def remote(self, *args, **kwargs):
        # Actually instantiate the actor
        ref = ray.call_remote(self, *args, **kwargs)
        return ClientActorHandle(ClientActorNameRef(ref.id), self)

    def __repr__(self):
        return "ClientRemoteActor(%s, %s)" % (self._name, self._ref)

    def __getattr__(self, key):
        if key not in self.__dict__:
            raise AttributeError("Not a class attribute")
        raise NotImplementedError("static methods")

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        if self._ref is None:
            self._ref = ray.put(self.actor_cls)
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.ACTOR
        task.name = self._name
        task.payload_id = self._ref.id
        return task


class ClientActorHandle:
    """
    A stub created on the Ray Client to represent a remote
    actor that has been started on the cluster. This class
    is allowed to be passed around between remote functions.

    Attributes:
        actor_id: A reference to the running actor given to the client
        actor_class: A reference to the ClientActorClass that this actor was
            instantiated from
        _real_actor_handle: The Raylet-side ray.actor.ActorHandle
    """

    def __init__(self, actor_id: ClientActorNameRef,
                 actor_class: ClientActorClass):
        self.actor_id = actor_id
        self.actor_class = actor_class
        self._real_actor_handle = None

    def _get_ray_remote_impl(self):
        if self._real_actor_handle is None:
            self._real_actor_handle = ray.get_actor_from_object(self.actor_id)
        return self._real_actor_handle

    def __getstate__(self) -> Dict:
        state = {
            "actor_id": self.actor_id,
            "actor_class": self.actor_class,
            "_real_actor_handle": self._real_actor_handle,
        }
        return state

    def __setstate__(self, state: Dict) -> None:
        self.actor_id = state["actor_id"]
        self.actor_class = state["actor_class"]
        self._real_actor_handle = state["_real_actor_handle"]

    def __getattr__(self, key):
        return ClientRemoteMethod(self, key)

    def __repr__(self):
        return "ClientActorHandle(%s, %s, %s)" % (
            self.actor_id, self.actor_class, self._real_actor_handle)


class ClientRemoteMethod:
    """
    A stub for a method on a remote actor. Can be annotated with exection
    options.

    actor_handle: A reference to the ClientActorHandle that generated
        this method and will have this method called upon it.
    method_name: The name of this method
    """

    def __init__(self, actor_handle: ClientActorHandle, method_name: str):
        self.actor_handle = actor_handle
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        raise TypeError(f"Remote method cannot be called directly. "
                        "Use {self._name}.remote() instead")

    def _get_ray_remote_impl(self):
        return getattr(self.actor_handle._get_ray_remote_impl(),
                       self.method_name)

    def __getstate__(self) -> Dict:
        state = {
            "actor_handle": self.actor_handle,
            "method_name": self.method_name,
        }
        return state

    def __setstate__(self, state: Dict) -> None:
        self.actor_handle = state["actor_handle"]
        self.method_name = state["method_name"]

    def remote(self, *args, **kwargs):
        return ray.call_remote(self, *args, **kwargs)

    def __repr__(self):
        name = "%s.%s" % (self.actor_handle.actor_class._name,
                          self.method_name)
        return "ClientRemoteMethod(%s, %s)" % (name,
                                               self.actor_handle.actor_id)

    def _prepare_client_task(self) -> ray_client_pb2.ClientTask:
        task = ray_client_pb2.ClientTask()
        task.type = ray_client_pb2.ClientTask.METHOD
        task.name = self.method_name
        task.payload_id = self.actor_handle.actor_id.id
        return task


def convert_from_arg(pb) -> Any:
    if pb.local == ray_client_pb2.Arg.Locality.REFERENCE:
        return ClientObjectRef(pb.reference_id)
    elif pb.local == ray_client_pb2.Arg.Locality.INTERNED:
        return cloudpickle.loads(pb.data)

    raise Exception("convert_from_arg: Uncovered locality enum")


def convert_to_arg(val):
    out = ray_client_pb2.Arg()
    if isinstance(val, ClientObjectRef):
        out.local = ray_client_pb2.Arg.Locality.REFERENCE
        out.reference_id = val.id
    else:
        out.local = ray_client_pb2.Arg.Locality.INTERNED
        out.data = cloudpickle.dumps(val)
    return out
