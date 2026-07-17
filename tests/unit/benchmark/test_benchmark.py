#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc.allocators import GreedyAllocator, NaiveAllocator
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.benchmark.benchmark import run_benchmark
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.benchmark.sources.generator import RandomSource


def test_run_benchmark_basic() -> None:
    """Test basic run_benchmark function."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()

    campaign = run_benchmark(
        allocators=(allocator,),
        sources=(source,),
        iterations=1,
        variants=10,
    )

    assert campaign.num_reports >= 1
    assert campaign.num_results >= 1


def test_run_benchmark_multiple_allocators() -> None:
    """Test run_benchmark with multiple allocators."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator1 = GreedyAllocator()
    allocator2 = NaiveAllocator()

    campaign = run_benchmark(
        allocators=(allocator1, allocator2),
        sources=(source,),
        iterations=1,
        variants=10,
    )

    assert campaign.num_allocators == 2


def test_run_benchmark_multiple_iterations() -> None:
    """Test run_benchmark with multiple iterations."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()

    campaign = run_benchmark(
        allocators=(allocator,),
        sources=(source,),
        iterations=3,
        variants=10,
    )

    assert all(report.num_results == 3 for report in campaign.reports)


def test_run_benchmark_metadata() -> None:
    """Test that run_benchmark includes metadata."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()

    campaign = run_benchmark(
        allocators=(allocator,),
        sources=(source,),
        iterations=1,
        variants=10,
    )

    assert "total_duration" in campaign.metadata
    assert "num_reports" in campaign.metadata


def test_run_benchmark_per_source_variants() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()

    campaign = run_benchmark(
        allocators=(allocator,),
        sources=(source,),
        iterations=1,
        variants={"random": (5, 10)},
    )

    assert campaign.num_reports == 2
    assert {r.variant_id for r in campaign.reports} == {5, 10}


def test_run_benchmark_on_vector_clock_source() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(source,),
        iterations=1,
        variants=16,
        validate=True,
    )

    assert campaign.num_reports == 1
    assert campaign.reports[0].mean_allocation_efficiency > 0


def test_run_benchmark_skips_scalar_only_allocators_on_vector_source() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    campaign = run_benchmark(
        allocators=(SupermallocAllocator(), GreedyAllocator()),
        sources=(source,),
        iterations=1,
        variants=16,
    )

    assert campaign.num_reports == 1
    assert campaign.reports[0].allocator_name == "greedy"


def test_run_benchmark_skips_unsupported_variants() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=4, num_syncs=8)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(source,),
        iterations=1,
        variants=(2, 16),
    )

    assert campaign.num_reports == 1
    assert campaign.reports[0].variant_id == 16


def test_run_benchmark_raises_when_all_pairs_skipped() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    with pytest.raises(ValueError, match="No benchmark reports"):
        run_benchmark(
            allocators=(SupermallocAllocator(),),
            sources=(source,),
            iterations=1,
            variants=16,
        )
