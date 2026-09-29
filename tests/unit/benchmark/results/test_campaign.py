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
from omnimalloc.benchmark.sources import BaseSource, RandomSource, SyncPatternSource


def _report(report_id: int, source: BaseSource | None = None) -> BenchmarkReport:
    source = source or RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=allocate(source.get_pool(), allocator),
        duration=0.5,
    )
    return BenchmarkReport(id=report_id, results=(result,), source=source)


def test_benchmark_campaign_counts() -> None:
    campaign = BenchmarkCampaign(id="c", reports=(_report(0), _report(1)))

    assert campaign.num_reports == 2
    assert campaign.num_results == 2
    assert campaign.num_allocators == 1
    assert campaign.allocator_names == ("greedy",)


def test_benchmark_campaign_empty_reports_raises_error() -> None:
    with pytest.raises(ValueError, match="must contain at least one report"):
        BenchmarkCampaign(id="c", reports=())


def test_benchmark_campaign_duplicate_report_ids_raises_error() -> None:
    with pytest.raises(ValueError, match="report ids must be unique"):
        BenchmarkCampaign(id="c", reports=(_report(0), _report(0)))


def test_benchmark_campaign_keeps_thread_counts_apart() -> None:
    few = SyncPatternSource(num_allocations=16, num_threads=2)
    many = SyncPatternSource(num_allocations=16, num_threads=8)
    campaign = BenchmarkCampaign(id="c", reports=(_report(0, few), _report(1, many)))

    assert campaign.num_sources == 2
    assert campaign.source_names == tuple(sorted((few.label(), many.label())))


def test_benchmark_campaign_groups_unlabelled_sources_together() -> None:
    campaign = BenchmarkCampaign(id="c", reports=(_report(0), _report(1)))
    assert campaign.source_names == ("random",)
