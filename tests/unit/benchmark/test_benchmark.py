#
# SPDX-License-Identifier: Apache-2.0
#


from omnimalloc.allocators import GreedyAllocator, NaiveAllocator
from omnimalloc.benchmark.benchmark import benchmark_campaign, run_benchmark
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


def test_benchmark_campaign_alias() -> None:
    """Test that benchmark_campaign is an alias for run_benchmark."""
    assert benchmark_campaign is run_benchmark
