#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

from omnimalloc.benchmark import run_benchmark, save_benchmark


def main() -> None:
    # Allocators whose optional library is missing are skipped, not fatal
    allocators = (
        "greedy_by_size",
        "greedy_by_all",
        "omni",
        "best_fit",
        "telamalloc",
        "minimalloc",
    )

    campaign = run_benchmark(
        allocators=allocators,
        sources=("random", "minimalloc", "huggingface"),
        # Counts for the parameterizable source, "first 5" for the Minimalloc
        # one; the Hugging Face source downloads a single model by default
        variants={"random": (10, 50, 100, 250, 500), "minimalloc": 5},
    )

    # Writes the overview plot, a results CSV, and one plot per iteration
    save_benchmark(campaign, Path("05_example_output") / "benchmark_results")


if __name__ == "__main__":
    main()
