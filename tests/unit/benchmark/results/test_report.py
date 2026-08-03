#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc import allocate
from omnimalloc.allocators import GreedyAllocator, NaiveAllocator
from omnimalloc.benchmark.results import BenchmarkReport, BenchmarkResult
from omnimalloc.benchmark.sources.generator import RandomSource
from omnimalloc.benchmark.sources.sync_patterns import SyncPatternSource


def test_benchmark_report_creation() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )

    report = BenchmarkReport(id=0, results=(result,))
    assert report.num_results == 1
    assert report.num_allocations == 10


def test_benchmark_report_empty_results_raises_error() -> None:
    with pytest.raises(ValueError, match="must contain at least one result"):
        BenchmarkReport(id=0, results=())


def test_benchmark_report_duplicate_ids_raises_error() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

    result1 = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    result2 = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.6
    )

    with pytest.raises(ValueError, match="result ids must be unique"):
        BenchmarkReport(id=0, results=(result1, result2))


def test_benchmark_report_statistics() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

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
    source = RandomSource(num_allocations=10, seed=42)
    allocator1 = GreedyAllocator()
    allocator2 = NaiveAllocator()

    pool1 = allocate(source.get_pool(), allocator1)
    pool2 = allocate(source.get_pool(), allocator2)

    result1 = BenchmarkResult(
        id=0, allocator=allocator1, source=source, entity=pool1, duration=0.5
    )
    result2 = BenchmarkResult(
        id=1, allocator=allocator2, source=source, entity=pool2, duration=0.6
    )

    with pytest.raises(ValueError, match="Allocator mismatch"):
        BenchmarkReport(id=0, results=(result1, result2), allocator=allocator1)


def test_benchmark_report_with_results() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

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


def _report(durations: tuple[float, ...], **overrides: object) -> BenchmarkReport:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    results = tuple(
        BenchmarkResult(
            id=i, allocator=allocator, source=source, entity=pool, duration=d
        )
        for i, d in enumerate(durations)
    )
    return BenchmarkReport(id=0, results=results, **overrides)  # type: ignore[arg-type]


def test_benchmark_report_dispersion_statistics() -> None:
    report = _report((1.0, 2.0, 3.0))

    assert report.min_seconds == 1.0
    assert report.max_seconds == 3.0
    assert report.stdev_seconds == pytest.approx(1.0)


def test_benchmark_report_stdev_is_none_for_single_iteration() -> None:
    report = _report((1.0,))

    assert report.stdev_seconds is None
    assert report.min_seconds == report.max_seconds == 1.0


def test_benchmark_report_peak_size_and_lower_bound() -> None:
    report = _report((1.0,))

    assert report.lower_bound > 0
    assert report.mean_peak_size >= report.lower_bound


def test_benchmark_report_optimum_ratio_absent_without_known_optimum() -> None:
    assert _report((1.0,)).known_optimum is None
    assert _report((1.0,)).optimum_ratio is None


def test_benchmark_report_optimum_ratio_compares_peak_to_optimum() -> None:
    report = _report((1.0,))
    with_optimum = BenchmarkReport(
        id=0, results=report.results, known_optimum=int(report.mean_peak_size) // 2
    )

    assert with_optimum.optimum_ratio == pytest.approx(2.0, rel=0.01)


def test_benchmark_report_with_results_keeps_known_optimum() -> None:
    report = _report((1.0,), known_optimum=1024)
    extended = report.with_results(
        (
            BenchmarkResult(
                id=99,
                allocator=report.results[0].allocator,
                source=report.results[0].source,
                entity=report.results[0].entity,
                duration=2.0,
            ),
        )
    )

    assert extended.known_optimum == 1024
    assert extended.stdev_seconds is not None


def test_benchmark_report_source_name_uses_instance_label() -> None:
    source = SyncPatternSource(num_allocations=16, num_threads=8)
    pool = allocate(source.get_pool(), GreedyAllocator())
    result = BenchmarkResult(
        id=0, allocator=GreedyAllocator(), source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,), source=source)

    assert report.source_name == source.label()
    assert "num_threads=8" in report.source_name
