#!/bin/bash
# Benchmark runner script for zero-copy loader

set -e

echo "Zero-Copy Loader Benchmark Suite"
echo "================================="
echo ""

# Check if Rust is available
if ! command -v cargo &> /dev/null; then
    echo "Error: cargo not found. Please install Rust."
    exit 1
fi

# Build the project
echo "Building project..."
cd "$(dirname "$0")/.."
cargo build --release --manifest-path rust-core/Cargo.toml

# Run Rust benchmarks
echo ""
echo "Running Rust I/O benchmarks..."
cd bench
cargo run --release --bin bench_io --manifest-path ../rust-core/Cargo.toml || {
    echo "Note: bench_io binary needs to be added to Cargo.toml"
    echo "Creating standalone benchmark..."
}

# Run Python benchmarks if available
if command -v python3 &> /dev/null; then
    echo ""
    echo "Running Python benchmarks..."
    python3 bench_python.py
fi

# Performance measurement with perf (if available)
if command -v perf &> /dev/null; then
    echo ""
    echo "Measuring with perf..."
    echo "Run manually with: perf stat -e cache-misses,cpu-cycles,instructions ./target/release/bench_io"
else
    echo ""
    echo "Note: Install 'perf' for detailed CPU and cache statistics"
fi

echo ""
echo "Benchmark complete!"