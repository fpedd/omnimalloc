#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc import allocate
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results import (
    BenchmarkCampaign,
    BenchmarkReport,
    BenchmarkResult,
)
from omnimalloc.benchmark.sources.generator import RandomSource
from omnimalloc.benchmark.sources.sync_patterns import SyncPatternSource


def test_benchmark_campaign_creation() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    campaign = BenchmarkCampaign(id="campaign_0", reports=(report,))

    assert campaign.num_reports == 1
    assert campaign.num_results == 1


def test_benchmark_campaign_empty_reports_raises_error() -> None:
    with pytest.raises(ValueError, match="must contain at least one report"):
        BenchmarkCampaign(id="campaign_0", reports=())


def test_benchmark_campaign_duplicate_report_ids_raises_error() -> None:
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
    report2 = BenchmarkReport(id=0, results=(result2,))

    with pytest.raises(ValueError, match="report ids must be unique"):
        BenchmarkCampaign(id="campaign_0", reports=(report1, report2))


def test_benchmark_campaign_properties() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

    results = tuple(
        BenchmarkResult(
            id=i, allocator=allocator, source=source, entity=pool, duration=0.5
        )
        for i in range(3)
    )
    report = BenchmarkReport(id=0, results=results)
    campaign = BenchmarkCampaign(id="campaign_0", reports=(report,))

    assert campaign.num_results == 3
    assert campaign.num_allocations == 30
    assert campaign.num_allocations_per_result == 10
    assert campaign.num_allocators == 1
    assert campaign.num_sources == 1


def test_benchmark_campaign_metadata_deep_copies_nested_values() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    metadata = {"tags": ["a"]}
    campaign = BenchmarkCampaign(id="campaign_0", reports=(report,), metadata=metadata)

    metadata["tags"].append("b")

    assert campaign.metadata == {"tags": ["a"]}


def test_benchmark_campaign_finalize_metadata() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)

    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    campaign = BenchmarkCampaign(
        id="campaign_0", reports=(report,), metadata={"custom": "value"}
    )
    finalized = campaign.finalize_metadata()

    assert "custom" in finalized.metadata
    assert "num_reports" in finalized.metadata


def _report(
    source: SyncPatternSource, report_id: int, result_id: int
) -> BenchmarkReport:
    pool = allocate(source.get_pool(), GreedyAllocator())
    result = BenchmarkResult(
        id=result_id,
        allocator=GreedyAllocator(),
        source=source,
        entity=pool,
        duration=0.5,
    )
    return BenchmarkReport(id=report_id, results=(result,), source=source)


def test_benchmark_campaign_keeps_thread_counts_apart() -> None:
    few = SyncPatternSource(num_allocations=16, num_threads=2)
    many = SyncPatternSource(num_allocations=16, num_threads=8)
    campaign = BenchmarkCampaign(
        id="campaign_0", reports=(_report(few, 0, 0), _report(many, 1, 1))
    )

    assert campaign.num_sources == 2
    assert campaign.source_names == tuple(sorted((few.label(), many.label())))
    assert set(campaign.reports_by_source_allocator_variant) == set(
        campaign.source_names
    )


def test_benchmark_campaign_groups_unlabelled_sources_together() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    pool = allocate(source.get_pool(), GreedyAllocator())
    reports = tuple(
        BenchmarkReport(
            id=i,
            results=(
                BenchmarkResult(
                    id=i,
                    allocator=GreedyAllocator(),
                    source=source,
                    entity=pool,
                    duration=0.5,
                ),
            ),
            source=source,
        )
        for i in range(2)
    )
    campaign = BenchmarkCampaign(id="campaign_0", reports=reports)

    assert campaign.source_names == ("random",)


def test_benchmark_campaign_rejects_non_copyable_metadata_with_clear_error() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = allocate(source.get_pool(), allocator)
    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    with pytest.raises(TypeError, match="deep-copyable"):
        BenchmarkCampaign(
            id="c", reports=(report,), metadata={"gen": (i for i in range(1))}
        )
