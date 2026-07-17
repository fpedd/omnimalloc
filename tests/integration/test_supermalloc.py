#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.allocators.minimalloc import HAS_MINIMALLOC, MinimallocAllocator
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.benchmark import plot_benchmark, run_benchmark, save_benchmark
from omnimalloc.visualize import HAS_MATPLOTLIB

ALLOCATORS = (
    "greedy_by_size",
    "greedy_by_all",
    "omni",
    SupermallocAllocator(timeout=2),
) + ((MinimallocAllocator(timeout=2),) if HAS_MINIMALLOC else ())

SIZE_VARIANTS = (64, 128, 256, 512, 1024)

MINIMALLOC_VARIANTS = tuple(f"{name}.1048576" for name in "ABCDEFGHIJK")


@pytest.mark.skipif(not HAS_MATPLOTLIB, reason="matplotlib not installed")
@pytest.mark.parametrize(
    ("source", "variants"),
    [
        ("minimalloc", MINIMALLOC_VARIANTS),
        ("tiling", SIZE_VARIANTS),
        ("pinwheel", SIZE_VARIANTS),
        ("random", SIZE_VARIANTS),
    ],
)
def test_benchmark(
    source: str, variants: tuple[int | str, ...], artifacts_dir: Path
) -> None:
    campaign = run_benchmark(
        allocators=ALLOCATORS,
        sources=(source,),
        variants=variants,
        validate=True,
    )
    assert len(campaign.reports) == len(ALLOCATORS) * len(variants)

    plot_file = artifacts_dir / "benchmark.pdf"
    plot_benchmark(campaign, plot_file)
    assert plot_file.exists()

    saved_path = save_benchmark(campaign, artifacts_dir / "results")
    assert saved_path.exists()
