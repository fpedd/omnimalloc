#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc import run_allocation
from omnimalloc.allocators import GreedyAllocator, NaiveAllocator
from omnimalloc.benchmark.results import BenchmarkReport, BenchmarkResult
from omnimalloc.benchmark.sources.generator import RandomSource


def test_benchmark_report_creation() -> None:
    """Test basic benchmark report creation."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )

    report = BenchmarkReport(id=0, results=(result,))
    assert report.num_results == 1
    assert report.num_allocations == 10


def test_benchmark_report_empty_results_raises_error() -> None:
    """Test that empty results raises ValueError."""
    with pytest.raises(ValueError, match="must contain at least one result"):
        BenchmarkReport(id=0, results=())


def test_benchmark_report_duplicate_ids_raises_error() -> None:
    """Test that duplicate result IDs raise ValueError."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

    result1 = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    result2 = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.6
    )

    with pytest.raises(ValueError, match="result ids must be unique"):
        BenchmarkReport(id=0, results=(result1, result2))


def test_benchmark_report_statistics() -> None:
    """Test report statistics with multiple results."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

    results = tuple(
        BenchmarkResult(
            id=i, allocator=allocator, source=source, entity=pool, duration=float(i)
        )
        for i in range(3)
    )

    report = BenchmarkReport(id=0, results=results)
    assert report.mean_seconds > 0
    assert report.median_seconds > 0
    assert 0.0 <= report.mean_allocation_efficiency <= 1.0


def test_benchmark_report_allocator_mismatch_raises_error() -> None:
    """Test that allocator mismatch raises ValueError."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator1 = GreedyAllocator()
    allocator2 = NaiveAllocator()

    pool1 = run_allocation(source.get_pool(), allocator1)
    pool2 = run_allocation(source.get_pool(), allocator2)

    result1 = BenchmarkResult(
        id=0, allocator=allocator1, source=source, entity=pool1, duration=0.5
    )
    result2 = BenchmarkResult(
        id=1, allocator=allocator2, source=source, entity=pool2, duration=0.6
    )

    with pytest.raises(ValueError, match="Allocator mismatch"):
        BenchmarkReport(id=0, results=(result1, result2), allocator=allocator1)


def test_benchmark_report_with_results() -> None:
    """Test with_results method."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

    result1 = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    result2 = BenchmarkResult(
        id=1, allocator=allocator, source=source, entity=pool, duration=0.6
    )

    report1 = BenchmarkReport(id=0, results=(result1,))
    report2 = report1.with_results((result2,))

    assert len(report1.results) == 1
    assert len(report2.results) == 2
