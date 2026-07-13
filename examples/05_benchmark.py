#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

from omnimalloc.allocators.minimalloc import HAS_MINIMALLOC
from omnimalloc.benchmark import (
    plot_benchmark,
    run_benchmark,
    save_benchmark,
)


def main() -> None:
    example_dir = Path("05_example_output")

    # Define allocators, sources, and variants to benchmark
    allocators = (
        "greedy_by_size_allocator",
        "greedy_by_size_allocator_cpp",
        "greedy_by_all_allocator_cpp",
        "best_fit_allocator",
        "telamalloc_allocator",
    )
    # minimalloc is an optional dependency that only builds on some platforms
    if HAS_MINIMALLOC:
        allocators += ("minimalloc_allocator",)
    sources = (
        "random_source",
        "minimalloc_source",
        "huggingface_source",
    )
    # Counts for the parameterizable source, "first 5" for the fixed ones
    variants = {
        "random_source": (10, 50, 100, 250, 500),
        "minimalloc_source": 5,
        "huggingface_source": 5,
    }

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


if __name__ == "__main__":
    main()
