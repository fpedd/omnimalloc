#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

from omnimalloc.benchmark import (
    plot_benchmark,
    run_benchmark,
    save_benchmark,
)

example_dir = Path("05_example_output")

# Define allocators, sources, and variants to benchmark
allocators = (
    "greedy_by_size_allocator",
    "greedy_by_size_allocator_cpp",
    "greedy_by_conflict_allocator",
    "minimalloc_allocator",
)
sources = (
    "random_source",
    "minimalloc_source",
    "huggingface_source",
)
variants = (10, 50, 100, 250, 500)

# Run benchmark campaign
campaign = run_benchmark(
    allocators=allocators,
    sources=sources,
    variants=variants,
    validate=True,
)

# Visualize
plot_benchmark(campaign, example_dir / "benchmark_results.pdf")

# Save results (contains overview and individual allocation plots)
save_benchmark(campaign, example_dir / "benchmark_results")
