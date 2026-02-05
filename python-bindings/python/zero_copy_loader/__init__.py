"""Zero-copy data loader for machine learning datasets.

This module provides a high-performance data loader using mmap and io_uring
for zero-copy I/O operations.
"""

from ._zero_copy_loader import PyDataLoader
import numpy as np
from typing import List, Union, Optional, Tuple

__all__ = ["DataLoader", "to_numpy", "to_torch"]


class DataLoader:
    """Zero-copy data loader for sharded binary datasets.

    This loader uses memory-mapped files and io_uring for efficient
    zero-copy data access, eliminating unnecessary CPU copies.

    Example:
        >>> loader = DataLoader(["shard1.bin", "shard2.bin"])
        >>> sample = loader.get_sample(0)
        >>> array = to_numpy(sample, dtype=np.float32, shape=(224, 224, 3))
    """

    def __init__(self, shard_paths: List[str]):
        """Initialize the data loader.

        Args:
            shard_paths: List of paths to shard files
        """
        self._loader = PyDataLoader(shard_paths)

    def get_sample(self, index: int) -> memoryview:
        """Get a sample by index (zero-copy).

        Args:
            index: Sample index

        Returns:
            memoryview: Zero-copy view of the sample data
        """
        bytes_obj = self._loader.get_sample(index)
        return memoryview(bytes_obj)

    def get_batch(self, indices: List[int]) -> List[memoryview]:
        """Get multiple samples at once (zero-copy).

        Args:
            indices: List of sample indices

        Returns:
            List of memoryview objects (zero-copy)
        """
        bytes_list = self._loader.get_batch(indices)
        return [memoryview(b) for b in bytes_list]

    def prefetch_next(self, count: int = 1) -> None:
        """Prefetch the next N shards asynchronously.

        Args:
            count: Number of shards to prefetch
        """
        self._loader.prefetch_next(count)

    def wait_prefetch(self) -> None:
        """Wait for prefetch operations to complete."""
        self._loader.wait_prefetch()

    @property
    def total_samples(self) -> int:
        """Total number of samples across all shards."""
        return self._loader.total_samples()

    @property
    def num_shards(self) -> int:
        """Number of shard files."""
        return self._loader.num_shards()


def to_numpy(
    buffer: memoryview,
    dtype: np.dtype,
    shape: Union[Tuple[int, ...], int],
) -> np.ndarray:
    """Convert a memoryview to a NumPy array (zero-copy when possible).

    Args:
        buffer: Memoryview of the data
        dtype: NumPy dtype (e.g., np.float32, np.uint8)
        shape: Shape of the array

    Returns:
        NumPy array (zero-copy if possible)
    """
    if isinstance(shape, int):
        shape = (shape,)

    # Use frombuffer for zero-copy conversion
    array = np.frombuffer(buffer, dtype=dtype)
    return array.reshape(shape)


def to_torch(
    buffer: memoryview,
    dtype: str,
    shape: Union[Tuple[int, ...], int],
) -> Optional[object]:
    """Convert a memoryview to a PyTorch tensor (zero-copy when possible).

    Args:
        buffer: Memoryview of the data
        dtype: PyTorch dtype string (e.g., "float32", "uint8")
        shape: Shape of the tensor

    Returns:
        PyTorch tensor (zero-copy if possible), or None if PyTorch is not available
    """
    try:
        import torch
    except ImportError:
        return None

    if isinstance(shape, int):
        shape = (shape,)

    # Convert to NumPy first, then to PyTorch
    np_dtype = {
        "float32": np.float32,
        "float64": np.float64,
        "int32": np.int32,
        "int64": np.int64,
        "uint8": np.uint8,
    }.get(dtype, np.float32)

    array = to_numpy(buffer, np_dtype, shape)
    torch_dtype = {
        "float32": torch.float32,
        "float64": torch.float64,
        "int32": torch.int32,
        "int64": torch.int64,
        "uint8": torch.uint8,
    }.get(dtype, torch.float32)

    return torch.from_numpy(array).to(torch_dtype)