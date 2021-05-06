# yapf: disable
from typing import Any, Callable, Generic, Optional, TypeVar, Union, overload, Sequence, List

from ray._raylet import ObjectRef


T0 = TypeVar("T0")
T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")
R = TypeVar("R")


class RemoteFunction(Generic[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]) -> None: pass

    @overload
    def remote(self) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]], arg4: Union[T4, ObjectRef[T4]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]], arg4: Union[T4, ObjectRef[T4]], arg5: Union[T5, ObjectRef[T5]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]], arg4: Union[T4, ObjectRef[T4]], arg5: Union[T5, ObjectRef[T5]], arg6: Union[T6, ObjectRef[T6]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]], arg4: Union[T4, ObjectRef[T4]], arg5: Union[T5, ObjectRef[T5]], arg6: Union[T6, ObjectRef[T6]], arg7: Union[T7, ObjectRef[T7]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]], arg4: Union[T4, ObjectRef[T4]], arg5: Union[T5, ObjectRef[T5]], arg6: Union[T6, ObjectRef[T6]], arg7: Union[T7, ObjectRef[T7]], arg8: Union[T8, ObjectRef[T8]]) -> ObjectRef[R]: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef[T0]], arg1: Union[T1, ObjectRef[T1]], arg2: Union[T2, ObjectRef[T2]], arg3: Union[T3, ObjectRef[T3]], arg4: Union[T4, ObjectRef[T4]], arg5: Union[T5, ObjectRef[T5]], arg6: Union[T6, ObjectRef[T6]], arg7: Union[T7, ObjectRef[T7]], arg8: Union[T8, ObjectRef[T8]], arg9: Union[T9, ObjectRef[T9]]) -> ObjectRef[R]: ...
    def remote(self, *args, **kwargs) -> ObjectRef[R]: ...


@overload
def remote(function: Callable[[], R]) -> RemoteFunction[R, None, None, None, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0], R]) -> RemoteFunction[R, T0, None, None, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1], R]) -> RemoteFunction[R, T0, T1, None, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2], R]) -> RemoteFunction[R, T0, T1, T2, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3], R]) -> RemoteFunction[R, T0, T1, T2, T3, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4], R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5], R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, T5, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, T5, T6, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, T5, T6, T7, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]: ...
# Pass on typing actors for now. The following makes it so no type errors are generated for actors.
@overload
def remote(t: type) -> Any: ...
def remote(function: Callable[..., R]) -> RemoteFunction[R, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]: ...



@overload
def get(object_refs: Sequence[ObjectRef[Any]], timeout: Optional[float] = None) -> List[Any]: ...
@overload
def get(object_refs: Sequence[ObjectRef[R]], timeout: Optional[float] = None) -> List[R]: ...
@overload
def get(object_refs: ObjectRef[R], timeout: Optional[float] = None) -> R: ...


ValueType = TypeVar("ValueType")
@overload
def put(value: ValueType) -> ObjectRef[ValueType]: ...