#!/usr/bin/env python3
"""Python benchmark for zero-copy loader.

This benchmark compares the zero-copy loader with PyTorch DataLoader
for ML workload simulation.
"""

import time
import numpy as np
from typing import List
import sys
import os

# Add the python-bindings to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-bindings', 'python'))

try:
    from zero_copy_loader import DataLoader, to_numpy
    ZERO_COPY_AVAILABLE = True
except ImportError:
    print("Warning: zero_copy_loader not available. Install it first.")
    ZERO_COPY_AVAILABLE = False

try:
    import torch
    from torch.utils.data import Dataset, DataLoader as TorchDataLoader
    TORCH_AVAILABLE = True
except ImportError:
    print("Warning: PyTorch not available. Skipping PyTorch comparison.")
    TORCH_AVAILABLE = False


def benchmark_zero_copy_loader(shard_paths: List[str], num_samples: int, batch_size: int = 32):
    """Benchmark the zero-copy loader."""
    if not ZERO_COPY_AVAILABLE:
        return None, None

    loader = DataLoader(shard_paths)

    # Warmup
    for i in range(min(10, num_samples)):
        _ = loader.get_sample(i)

    # Benchmark
    start_time = time.time()
    total_bytes = 0

    for i in range(0, num_samples, batch_size):
        indices = list(range(i, min(i + batch_size, num_samples)))
        batch = loader.get_batch(indices)
        for sample in batch:
            total_bytes += len(sample)

    elapsed = time.time() - start_time
    throughput = total_bytes / elapsed / 1024 / 1024  # MB/s

    return elapsed, throughput


def benchmark_torch_dataloader(shard_paths: List[str], num_samples: int, batch_size: int = 32):
    """Benchmark PyTorch DataLoader (simulated)."""
    if not TORCH_AVAILABLE:
        return None, None

    # Create a simple dataset that reads from files
    class SimpleDataset(Dataset):
        def __init__(self, num_samples):
            self.num_samples = num_samples

        def __len__(self):
            return self.num_samples

        def __getitem__(self, idx):
            # Simulate reading a sample (in real scenario, this would read from disk)
            return torch.randn(224, 224, 3)  # Simulated image data

    dataset = SimpleDataset(num_samples)
    loader = TorchDataLoader(dataset, batch_size=batch_size, num_workers=0)

    # Warmup
    for i, _ in enumerate(loader):
        if i >= 10:
            break

    # Benchmark
    start_time = time.time()
    total_bytes = 0

    for batch in loader:
        total_bytes += batch.numel() * batch.element_size()

    elapsed = time.time() - start_time
    throughput = total_bytes / elapsed / 1024 / 1024  # MB/s

    return elapsed, throughput


def main():
    print("Python Benchmark for Zero-Copy Loader")
    print("=" * 50)
    print()

    # Note: In a real benchmark, you would create actual shard files
    # For this example, we'll just show the structure
    print("Note: This benchmark requires actual shard files.")
    print("Create shard files using the Rust benchmark tool first.")
    print()

    if not ZERO_COPY_AVAILABLE:
        print("ERROR: zero_copy_loader is not available.")
        print("Build it with: cd python-bindings && maturin develop")
        return

    # Example usage (would need actual files)
    print("Example usage:")
    print("  loader = DataLoader(['shard1.bin', 'shard2.bin'])")
    print("  sample = loader.get_sample(0)")
    print("  array = to_numpy(sample, dtype=np.float32, shape=(224, 224, 3))")
    print()

    if TORCH_AVAILABLE:
        print("PyTorch comparison:")
        print("  The zero-copy loader eliminates unnecessary copies")
        print("  that occur in traditional data loading pipelines.")
        print("  Use perf to measure CPU usage and copy counts:")
        print("    perf stat -e cache-misses,cpu-cycles python bench_python.py")


if __name__ == "__main__":
    main()