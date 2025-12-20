#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc import run_allocation
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results import (
    BenchmarkCampaign,
    BenchmarkReport,
    BenchmarkResult,
)
from omnimalloc.benchmark.sources.generator import RandomSource


def test_benchmark_campaign_creation() -> None:
    """Test basic benchmark campaign creation."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

    result = BenchmarkResult(
        id=0, allocator=allocator, source=source, entity=pool, duration=0.5
    )
    report = BenchmarkReport(id=0, results=(result,))
    campaign = BenchmarkCampaign(id="campaign_0", reports=(report,))

    assert campaign.num_reports == 1
    assert campaign.num_results == 1


def test_benchmark_campaign_empty_reports_raises_error() -> None:
    """Test that empty reports raises ValueError."""
    with pytest.raises(ValueError, match="must contain at least one report"):
        BenchmarkCampaign(id="campaign_0", reports=())


def test_benchmark_campaign_duplicate_report_ids_raises_error() -> None:
    """Test that duplicate report IDs raise ValueError."""
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
    report2 = BenchmarkReport(id=0, results=(result2,))

    with pytest.raises(ValueError, match="report ids must be unique"):
        BenchmarkCampaign(id="campaign_0", reports=(report1, report2))


def test_benchmark_campaign_properties() -> None:
    """Test campaign properties."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

    results = tuple(
        BenchmarkResult(
            id=i, allocator=allocator, source=source, entity=pool, duration=0.5
        )
        for i in range(3)
    )
    report = BenchmarkReport(id=0, results=results)
    campaign = BenchmarkCampaign(id="campaign_0", reports=(report,))

    assert campaign.num_results == 3
    assert campaign.num_allocations == 10
    assert campaign.num_allocators == 1
    assert campaign.num_sources == 1


def test_benchmark_campaign_finalize_metadata() -> None:
    """Test finalize_metadata method."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = run_allocation(source.get_pool(), allocator)

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
