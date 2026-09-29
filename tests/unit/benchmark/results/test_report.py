#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import allocate
from omnimalloc.allocators import BaseAllocator, GreedyAllocator, NaiveAllocator
from omnimalloc.benchmark.results import BenchmarkReport, BenchmarkResult
from omnimalloc.benchmark.sources import BaseSource, RandomSource, SyncPatternSource


def _result(
    result_id: int = 0,
    duration: float = 1.0,
    allocator: BaseAllocator | None = None,
    source: BaseSource | None = None,
) -> BenchmarkResult:
    allocator = allocator or GreedyAllocator()
    source = source or RandomSource(num_allocations=10, seed=42)
    return BenchmarkResult(
        id=result_id,
        allocator=allocator,
        source=source,
        entity=allocate(source.get_pool(), allocator),
        duration=duration,
    )


def _report(*durations: float, **overrides: object) -> BenchmarkReport:
    results = tuple(_result(i, d) for i, d in enumerate(durations))
    return BenchmarkReport(id=0, results=results, **overrides)  # type: ignore[arg-type]


def test_benchmark_report_statistics() -> None:
    report = _report(1.0, 2.0, 3.0)

    assert report.num_results == 3
    assert report.num_allocations == 10
    assert report.mean_seconds == report.median_seconds == 2.0
    assert report.min_seconds == 1.0
    assert report.max_seconds == 3.0
    assert report.stdev_seconds == pytest.approx(1.0)
    assert 0.0 <= report.mean_allocation_efficiency <= 1.0
    assert report.mean_peak_size >= report.lower_bound > 0


def test_benchmark_report_stdev_is_none_for_single_iteration() -> None:
    assert _report(1.0).stdev_seconds is None


def test_benchmark_report_empty_results_raises_error() -> None:
    with pytest.raises(ValueError, match="must contain at least one result"):
        BenchmarkReport(id=0, results=())


def test_benchmark_report_duplicate_ids_raises_error() -> None:
    with pytest.raises(ValueError, match="result ids must be unique"):
        BenchmarkReport(id=0, results=(_result(0), _result(0)))


def test_benchmark_report_allocator_mismatch_raises_error() -> None:
    results = (_result(0), _result(1, allocator=NaiveAllocator()))
    with pytest.raises(ValueError, match="Allocator mismatch"):
        BenchmarkReport(id=0, results=results, allocator=GreedyAllocator())


def test_benchmark_report_optimum_ratio() -> None:
    report = _report(1.0)
    half = int(report.mean_peak_size) // 2

    assert report.optimum_ratio is None
    assert _report(1.0, known_optimum=half).optimum_ratio == pytest.approx(2.0, 0.01)


@pytest.mark.parametrize(("variant_id", "label"), [(None, "10"), (7, "7"), ("m", "m")])
def test_benchmark_report_variant_label(variant_id: object, label: str) -> None:
    assert _report(1.0, variant_id=variant_id).variant_label == label


def test_benchmark_report_source_name_uses_instance_label() -> None:
    source = SyncPatternSource(num_allocations=16, num_threads=8)
    report = BenchmarkReport(id=0, results=(_result(source=source),), source=source)

    assert report.source_name == source.label()
    assert "num_threads=8" in report.source_name
