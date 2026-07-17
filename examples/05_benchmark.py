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
        "greedy_by_size",
        "greedy_by_all",
        "omni",
        "best_fit",
        "telamalloc",
    )
    # minimalloc is an optional dependency that only builds on some platforms
    if HAS_MINIMALLOC:
        allocators += ("minimalloc",)
    sources = (
        "random",
        "minimalloc",
        "huggingface",
    )
    # Counts for the parameterizable source, "first 5" for the fixed ones
    variants = {
        "random": (10, 50, 100, 250, 500),
        "minimalloc": 5,
        "huggingface": 5,
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
