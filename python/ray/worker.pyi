from typing import Any, Callable, Generic, Optional, TypeVar, Union, overload

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


class RemoteFunction(Generic[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]):
    def __init__(self, function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], Any]) -> None: pass

    @overload
    def remote(self) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef], arg4: Union[T4, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef], arg4: Union[T4, ObjectRef], arg5: Union[T5, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef], arg4: Union[T4, ObjectRef], arg5: Union[T5, ObjectRef], arg6: Union[T6, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef], arg4: Union[T4, ObjectRef], arg5: Union[T5, ObjectRef], arg6: Union[T6, ObjectRef], arg7: Union[T7, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef], arg4: Union[T4, ObjectRef], arg5: Union[T5, ObjectRef], arg6: Union[T6, ObjectRef], arg7: Union[T7, ObjectRef], arg8: Union[T8, ObjectRef]) -> ObjectRef: ...
    @overload
    def remote(self, arg0: Union[T0, ObjectRef], arg1: Union[T1, ObjectRef], arg2: Union[T2, ObjectRef], arg3: Union[T3, ObjectRef], arg4: Union[T4, ObjectRef], arg5: Union[T5, ObjectRef], arg6: Union[T6, ObjectRef], arg7: Union[T7, ObjectRef], arg8: Union[T8, ObjectRef], arg9: Union[T9, ObjectRef]) -> ObjectRef: ...
    def remote(self, *args, **kwargs) -> ObjectRef:
        pass


@overload
def remote(function: Callable[[], R]) -> RemoteFunction[None, None, None, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0], R]) -> RemoteFunction[T0, None, None, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1], R]) -> RemoteFunction[T0, T1, None, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2], R]) -> RemoteFunction[T0, T1, T2, None, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3], R]) -> RemoteFunction[T0, T1, T2, T3, None, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4], R]) -> RemoteFunction[T0, T1, T2, T3, T4, None, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5], R]) -> RemoteFunction[T0, T1, T2, T3, T4, T5, None, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6], R]) -> RemoteFunction[T0, T1, T2, T3, T4, T5, T6, None, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7], R]) -> RemoteFunction[T0, T1, T2, T3, T4, T5, T6, T7, None, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8], R]) -> RemoteFunction[T0, T1, T2, T3, T4, T5, T6, T7, T8, None]: ...
@overload
def remote(function: Callable[[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9], R]) -> RemoteFunction[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]: ...
# Pass on typing actors for now. The following makes it so no type errors are generated for actors.
@overload
def remote(t: type) -> Any: ...
def remote(function: Callable[..., R]) -> RemoteFunction[T0, T1, T2, T3, T4, T5, T6, T7, T8, T9]: pass
