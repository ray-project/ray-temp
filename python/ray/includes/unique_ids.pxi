"""This is a module for unique IDs in Ray.
We define different types for different IDs for type safety.

See https://github.com/ray-project/ray/issues/3721.
"""

# WARNING: Any additional ID types defined in this file must be added to the
# _ID_TYPES list at the bottom of this file.
from ray.includes.common cimport (
    ComputePutId,
    ComputeTaskId,
)
from ray.includes.unique_ids cimport (
    CActorCheckpointID,
    CActorClassID,
    CActorHandleID,
    CActorID,
    CClientID,
    CConfigID,
    CDriverID,
    CFunctionID,
    CJobID,
    CObjectID,
    CTaskID,
    CUniqueID,
    CWorkerID,
)

from ray.utils import decode


def check_id(b):
    if not isinstance(b, bytes):
        raise TypeError("Unsupported type: " + str(type(b)))
    if len(b) != kUniqueIDSize:
        raise ValueError("ID string needs to have length " +
                         str(kUniqueIDSize))


cdef extern from "ray/constants.h" nogil:
    cdef int64_t kUniqueIDSize
    cdef int64_t kMaxTaskPuts


cdef class UniqueID:
    cdef CUniqueID *data

    def __cinit__(self, id):
        # The derived class should also check self type and fill self.data.
        if type(self) is UniqueID:
            check_id(id)
            self.data = new CUniqueID(<c_string>id)

    def __dealloc__(self):
        # The derived classes do not need to define __dealloc__,
        # the base class will do it.
        del self.data

    @classmethod
    def from_binary(cls, id_bytes):
        if not isinstance(id_bytes, bytes):
            raise TypeError("Expect bytes, got " + str(type(id_bytes)))
        return cls(id_bytes)

    @classmethod
    def nil(cls):
        return cls(CUniqueID.nil().binary())

    def __hash__(self):
        return self.data.hash()

    def is_nil(self):
        return self.data.is_nil()

    def __eq__(self, other):
        return type(self) == type(other) and self.binary() == other.binary()

    def __ne__(self, other):
        return self.binary() != other.binary()

    def size(self):
        return self.data.size()

    def binary(self):
        return self.data.binary()

    def __bytes__(self):
        return self.binary()

    def hex(self):
        return decode(self.data.hex())

    def __hex__(self):
        return self.hex()

    def __repr__(self):
        return self.__class__.__name__ + "(" + self.hex() + ")"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.binary(),)

    def redis_shard_hash(self):
        # NOTE: The hash function used here must match the one in
        # GetRedisContext in src/ray/gcs/tables.h. Changes to the
        # hash function should only be made through std::hash in
        # src/common/common.h
        return self.data.hash()


cdef class ObjectID(UniqueID):

    def __cinit__(self, id):
        if type(self) is ObjectID:
            check_id(id)
            self.data = new CObjectID(<c_string>id)

    cdef CObjectID native(self):
        return (<CObjectID *>self.data)[0]


cdef class TaskID(UniqueID):

    def __cinit__(self, id):
        if type(self) is TaskID:
            check_id(id)
            self.data = new CTaskID(<c_string>id)

    cdef CTaskID native(self):
        return (<CTaskID *>self.data)[0]


cdef class ClientID(UniqueID):

    def __cinit__(self, id):
        if type(self) is ClientID:
            check_id(id)
            self.data = new CClientID(<c_string>id)

    cdef CClientID native(self):
        return (<CClientID *>self.data)[0]


cdef class DriverID(UniqueID):

    def __cinit__(self, id):
        if type(self) is DriverID:
            check_id(id)
            self.data = new CDriverID(<c_string>id)

    cdef CDriverID native(self):
        return (<CDriverID *>self.data)[0]


cdef class ActorID(UniqueID):

    def __cinit__(self, id):
        if type(self) is ActorID:
            check_id(id)
            self.data = new CActorID(<c_string>id)

    cdef CActorID native(self):
        return (<CActorID *>self.data)[0]


cdef class ActorHandleID(UniqueID):

    def __cinit__(self, id):
        if type(self) is ActorHandleID:
            check_id(id)
            self.data = new CActorHandleID(<c_string>id)

    cdef CActorHandleID native(self):
        return (<CActorHandleID *>self.data)[0]


cdef class ActorCheckpointID(UniqueID):

    def __cinit__(self, id):
        if type(self) is ActorCheckpointID:
            check_id(id)
            self.data = new CActorCheckpointID(<c_string>id)

    cdef CActorCheckpointID native(self):
        return (<CActorCheckpointID *>self.data)[0]


cdef class FunctionID(UniqueID):

    def __cinit__(self, id):
        if type(self) is FunctionID:
            check_id(id)
            self.data = new CFunctionID(<c_string>id)

    cdef CFunctionID native(self):
        return (<CFunctionID *>self.data)[0]


cdef class ActorClassID(UniqueID):

    def __cinit__(self, id):
        if type(self) is ActorClassID:
            check_id(id)
            self.data = new CActorClassID(<c_string>id)

    cdef CActorClassID native(self):
        return (<CActorClassID *>self.data)[0]

_ID_TYPES = [
    ActorCheckpointID,
    ActorClassID,
    ActorHandleID,
    ActorID,
    ClientID,
    DriverID,
    FunctionID,
    ObjectID,
    TaskID,
    UniqueID,
]
