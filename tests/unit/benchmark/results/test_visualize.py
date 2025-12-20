#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc import run_allocation
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results.campaign import BenchmarkCampaign
from omnimalloc.benchmark.results.report import BenchmarkReport
from omnimalloc.benchmark.results.result import BenchmarkResult
from omnimalloc.benchmark.results.visualize import (
    _canonicalize_artifact,
    _format_metadata,
    _get_allocator_color,
)
from omnimalloc.benchmark.sources.generator import RandomSource


def test_get_allocator_color() -> None:
    """Test allocator color cycling."""
    color_0 = _get_allocator_color(0)
    color_5 = _get_allocator_color(5)
    color_10 = _get_allocator_color(10)

    assert color_0 == "C0"
    assert color_5 == "C5"
    assert color_10 == "C0"  # Should cycle back to C0


def test_format_metadata() -> None:
    """Test metadata formatting."""
    metadata = {"model_name": "resnet50", "batch_size": 32, "dtype": "float32"}
    formatted = _format_metadata(metadata)

    assert "Model Name: resnet50" in formatted
    assert "Batch Size: 32" in formatted
    assert "Dtype: float32" in formatted
    assert " | " in formatted

    empty_formatted = _format_metadata(None)
    assert empty_formatted == ""

    empty_dict_formatted = _format_metadata({})
    assert empty_dict_formatted == ""


def test_canonicalize_artifact() -> None:
    """Test artifact canonicalization to campaign."""
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()
    pool = source.get_pool()
    allocated_pool = run_allocation(pool, allocator)

    result = BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=allocated_pool,
        duration=0.5,
    )

    campaign_from_result = _canonicalize_artifact(result)
    assert isinstance(campaign_from_result, BenchmarkCampaign)
    assert len(campaign_from_result.reports) == 1
    assert len(campaign_from_result.reports[0].results) == 1

    report = BenchmarkReport(id="report_0", results=(result,))
    campaign_from_report = _canonicalize_artifact(report)
    assert isinstance(campaign_from_report, BenchmarkCampaign)
    assert len(campaign_from_report.reports) == 1

    campaign = BenchmarkCampaign(id="campaign_0", reports=(report,))
    campaign_from_campaign = _canonicalize_artifact(campaign)
    assert isinstance(campaign_from_campaign, BenchmarkCampaign)
    assert campaign_from_campaign is campaign
