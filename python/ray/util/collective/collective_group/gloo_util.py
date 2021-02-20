"""Code to wrap some GLOO API calls."""
import numpy
from numpy.core.records import array
try:
    import pygloo
except ImportError:
    raise ImportError("Can not import pygloo. Please run 'pip install pygloo' to install pygloo.")

from ray.util.collective.types import ReduceOp, torch_available

GLOO_REDUCE_OP_MAP = {
    ReduceOp.SUM: pygloo.ReduceOp.SUM,
    ReduceOp.PRODUCT: pygloo.ReduceOp.PRODUCT,
    ReduceOp.MIN: pygloo.ReduceOp.MIN,
    ReduceOp.MAX: pygloo.ReduceOp.MAX,
}

# cupy types are the same with numpy types
NUMPY_GLOO_DTYPE_MAP = {
    # INT types
    numpy.int: pygloo.glooDataType_t.glooInt8,
    numpy.uint8: pygloo.glooDataType_t.glooUint8,
    numpy.uint32: pygloo.glooDataType_t.glooUint32,
    numpy.uint64: pygloo.glooDataType_t.glooUint64,
    numpy.int8: pygloo.glooDataType_t.glooInt8,
    numpy.int32: pygloo.glooDataType_t.glooInt32,
    numpy.int64: pygloo.glooDataType_t.glooInt64,
    # FLOAT types
    numpy.half: pygloo.glooDataType_t.glooFloat16,
    numpy.float: pygloo.glooDataType_t.glooFloat64,
    numpy.float16: pygloo.glooDataType_t.glooFloat16,
    numpy.float32: pygloo.glooDataType_t.glooFloat32,
    numpy.float64: pygloo.glooDataType_t.glooFloat64,
    numpy.double: pygloo.glooDataType_t.glooFloat64,
}

if torch_available():
    import torch
    TORCH_GLOO_DTYPE_MAP = {
        torch.int: pygloo.glooDataType_t.glooInt8,
        torch.uint8: pygloo.glooDataType_t.glooUint8,
        torch.int8: pygloo.glooDataType_t.glooInt8,
        torch.int32: pygloo.glooDataType_t.glooInt32,
        torch.int64: pygloo.glooDataType_t.glooInt64,
        torch.long: pygloo.glooDataType_t.glooInt64,
        # FLOAT types
        torch.half: pygloo.glooDataType_t.glooFloat16,
        torch.float: pygloo.glooDataType_t.glooFloat64,
        torch.float16: pygloo.glooDataType_t.glooFloat16,
        torch.float32: pygloo.glooDataType_t.glooFloat32,
        torch.float64: pygloo.glooDataType_t.glooFloat64,
        torch.double: pygloo.glooDataType_t.glooFloat64,
    }

    TORCH_NUMPY_DTYPE_MAP = {
        # INT types
        torch.int: numpy.int,
        torch.uint8: numpy.uint8,
        torch.int8: numpy.int8,
        torch.int32: numpy.int32,
        torch.int64: numpy.int64,
        torch.long: numpy.int64,
        # FLOAT types
        torch.half: numpy.half,
        torch.float: numpy.float,
        torch.float16: numpy.float16,
        torch.float32: numpy.float32,
        torch.float64: numpy.float64,
    }

# def get_gloo_build_version():
#     return get_build_version()


# def get_gloo_runtime_version():
#     return get_version()


def create_gloo_context(rank, world_size):
    """
    Create an gloo context using gloo APIs.

    Args:
        rank (int): the rank of this process.
        world_size (int): the number of processes of this communcator group.
    Returns:
        context (pygloo.Context): a gloo context.
    """
    context = pygloo.rendezvous.Context(rank, world_size);
    return context


def get_gloo_reduce_op(reduce_op):
    """
    Map the reduce op to GLOO reduce op type.

    Args:
        reduce_op (ReduceOp): ReduceOp Enum (SUM/PRODUCT/MIN/MAX).
    Returns:
        (pygloo.ReduceOp): the mapped gloo reduce op.
    """
    if reduce_op not in GLOO_REDUCE_OP_MAP:
        raise RuntimeError(
            "Gloo does not support reduce op: '{}'".format(reduce_op))
    return GLOO_REDUCE_OP_MAP[reduce_op]


def get_gloo_tensor_dtype(tensor):
    """Return the corresponded GLOO dtype given a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return NUMPY_GLOO_DTYPE_MAP[tensor.dtype.type]
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            if tensor.device == "cpu":
                return TORCH_GLOO_DTYPE_MAP[tensor.dtype]
            else:
                raise ValueError("Got gpu tensor. But gloo only accept cpu tensor.")
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_ptr(tensor):
    """Return the pointer to the underlying memory storage of a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.ctypes.data
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            if not tensor.is_cuda:
                raise RuntimeError("torch tensor must be on gpu.")
            return tensor.data_ptr()
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))


def get_tensor_n_elements(tensor):
    """Return the number of elements in a tensor."""
    if isinstance(tensor, numpy.ndarray):
        return tensor.size
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return torch.numel(tensor)
    raise ValueError("Unsupported tensor type. "
                     "Got: {}.".format(type(tensor)))

def get_gloo_store_path(store_name):
    from ray.utils import get_ray_temp_dir
    store_path = f"{get_ray_temp_dir()}_collective/gloo/{store_name}"
    return store_path

def get_tensor_device(tensor):
    if isinstance(tensor, numpy.ndarray):
        return 'cpu'
    elif torch_available() and isinstance(tensor, torch.Tensor):
        device = tensor.device
        return device

def get_tensor_shape(tensor):
    """Return the shape of the tensor as a list."""
    if isinstance(tensor, numpy.ndarray):
        return list(tensor.shape)
    if torch_available():
        if isinstance(tensor, torch.Tensor):
            return list(tensor.size())
    raise ValueError("Unsupported tensor type. Got: {}. Supported "
                     "CPU tensor types are: torch.Tensor, "
                     "numpy.ndarray.".format(type(tensor)))

def copy_tensor(dst_tensor, src_tensor):
    """Copy the content from src_tensor to dst_tensor.

    Args:
        dst_tensor: the tensor to copy from.
        src_tensor: the tensor to copy to.

    Returns:
        None
    """
    copied = True
    if isinstance(dst_tensor, numpy.ndarray) \
            and isinstance(src_tensor, numpy.ndarray):
        numpy.copyto(dst_tensor, src_tensor)
    elif torch_available():
        if isinstance(dst_tensor, torch.Tensor) and isinstance(
                src_tensor, torch.Tensor):
            dst_tensor.copy_(src_tensor)
        elif isinstance(dst_tensor, torch.Tensor) and isinstance(
                src_tensor, numpy.ndarray):
            # t = torch.utils.dlpack.from_dlpack(src_tensor.toDlpack())
            t = torch.Tensor(src_tensor)
            dst_tensor.copy_(t)
        elif isinstance(dst_tensor, numpy.ndarray) and isinstance(
                src_tensor, torch.Tensor):
            # t = numpy.fromDlpack(torch.utils.dlpack.to_dlpack(src_tensor))
            t = src_tensor.numpy()
            numpy.copyto(dst_tensor, t)
        else:
            copied = False
    else:
        copied = False
    if not copied:
        raise ValueError("Unsupported tensor type. Got: {} and {}. Supported "
                         "GPU tensor types are: torch.Tensor, cupy.ndarray."
                         .format(type(dst_tensor), type(src_tensor)))
