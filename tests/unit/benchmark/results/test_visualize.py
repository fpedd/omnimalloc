#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import allocate
from omnimalloc.allocators import GreedyAllocator
from omnimalloc.benchmark.results.campaign import BenchmarkCampaign
from omnimalloc.benchmark.results.report import BenchmarkReport
from omnimalloc.benchmark.results.result import BenchmarkResult
from omnimalloc.benchmark.results.visualize import (
    _canonicalize_artifact,
    _format_metadata,
    _get_allocator_color,
    _get_sorted_reports,
    plot_benchmark,
)
from omnimalloc.benchmark.sources.generator import RandomSource

from tests.markers import needs_matplotlib


def _result(size: int = 10) -> BenchmarkResult:
    source = RandomSource(num_allocations=size, seed=42)
    allocator = GreedyAllocator()
    return BenchmarkResult(
        id=0,
        allocator=allocator,
        source=source,
        entity=allocate(source.get_pool(), allocator),
        duration=0.5,
    )


def _report(report_id: str, variant_id: object, size: int = 10) -> BenchmarkReport:
    return BenchmarkReport(
        id=report_id, results=(_result(size),), variant_id=variant_id
    )


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
    result = _result()

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


@needs_matplotlib
def test_plot_benchmark_without_path_shows_figure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import matplotlib.pyplot as plt

    shown = []
    monkeypatch.setattr(plt, "show", lambda: shown.append(True))
    plot_benchmark(_result())
    assert shown == [True]


def test_sorted_reports_handles_mixed_variant_id_types() -> None:
    mixed = {"a": (_report("r0", "small"), _report("r1", 100), _report("r2", None))}
    assert len(_get_sorted_reports(mixed)) == 3


def test_sorted_reports_orders_numeric_variants_by_size() -> None:
    numeric = {
        "a": (
            _report("r0", 30, size=30),
            _report("r1", 10, size=10),
            _report("r2", 20, size=20),
        )
    }
    assert [r.variant_id for r in _get_sorted_reports(numeric)] == [10, 20, 30]


def test_sorted_reports_orders_categorical_variants_by_name() -> None:
    categorical = {"a": (_report("r0", "c"), _report("r1", "a"), _report("r2", "b"))}
    assert [r.variant_id for r in _get_sorted_reports(categorical)] == ["a", "b", "c"]
