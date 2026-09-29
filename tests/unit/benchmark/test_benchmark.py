#
# SPDX-License-Identifier: Apache-2.0
#

import inspect
from pathlib import Path

import pytest
from omnimalloc.allocators import GreedyAllocator, NaiveAllocator
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.benchmark.benchmark import run_benchmark
from omnimalloc.benchmark.results import BenchmarkCampaign
from omnimalloc.benchmark.sources import (
    ConcurrentTilingSource,
    MinimallocSource,
    PinwheelSource,
    RandomSource,
    SyncPatternSource,
    TilingSource,
)
from omnimalloc.io import save_allocation
from omnimalloc.primitives import Allocation, Pool


def _run(**kwargs: object) -> BenchmarkCampaign:
    """One greedy run on ten random allocations unless overridden."""
    defaults = {
        "allocators": (GreedyAllocator(),),
        "sources": (RandomSource(num_allocations=10, seed=42),),
    }
    return run_benchmark(**(defaults | kwargs))  # type: ignore[arg-type]


def _variants(campaign: BenchmarkCampaign, source: object) -> set[object]:
    return {r.variant_id for r in campaign.reports if r.source == source}


def _vector_source() -> ConcurrentTilingSource:
    return ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)


def test_run_benchmark_defaults() -> None:
    campaign = _run()

    assert inspect.signature(run_benchmark).parameters["validate"].default is True
    assert campaign.num_reports == campaign.num_results == 1
    assert campaign.reports[0].num_allocations == 10
    assert campaign.reports[0].known_optimum is None
    assert campaign.metadata["skipped"] == []
    assert {"total_duration", "num_reports", "omnimalloc_version"} <= set(
        campaign.metadata
    )


def test_run_benchmark_repeats_iterations_per_allocator() -> None:
    campaign = _run(allocators=(GreedyAllocator(), NaiveAllocator()), iterations=3)

    assert campaign.num_allocators == 2
    assert all(report.num_results == 3 for report in campaign.reports)


def test_run_benchmark_keeps_a_falsy_campaign_id() -> None:
    assert _run(campaign_id=0).id == 0


def test_run_benchmark_per_source_variants() -> None:
    campaign = _run(variants={"random": (5, 10)})
    assert {r.variant_id for r in campaign.reports} == {5, 10}


def test_run_benchmark_on_vector_clock_source() -> None:
    campaign = _run(sources=(_vector_source(),))
    assert campaign.reports[0].mean_allocation_efficiency > 0


def test_run_benchmark_skips_unsupported_allocators() -> None:
    source = _vector_source()
    campaign = _run(
        allocators=(SupermallocAllocator(), GreedyAllocator()), sources=(source,)
    )

    assert [r.allocator_name for r in campaign.reports] == ["greedy"]
    assert campaign.metadata["skipped"] == [
        {
            "source": source.label(),
            "variant": "16",
            "allocator": "supermalloc",
            "reason": (
                "supermalloc requires scalar (interval) lifetimes, "
                "got 2-dim vector clocks"
            ),
        }
    ]


def test_run_benchmark_raises_when_all_pairs_skipped() -> None:
    with pytest.raises(ValueError, match="No benchmark reports"):
        _run(allocators=(SupermallocAllocator(),), sources=(_vector_source(),))


def test_run_benchmark_skips_unreachable_variants_once() -> None:
    source = PinwheelSource(num_allocations=65)
    campaign = _run(
        allocators=(GreedyAllocator(), NaiveAllocator()),
        sources=(source,),
        variants=(64, 65),
    )

    assert _variants(campaign, source) == {65}
    assert campaign.metadata["skipped"] == [
        {
            "source": source.label(),
            "variant": "64",
            "reason": "cannot reach exactly 64 allocations, nearest is 65",
        }
    ]


def test_run_benchmark_records_known_optimum() -> None:
    campaign = _run(sources=(TilingSource(num_allocations=32, capacity=4096),))

    assert campaign.reports[0].known_optimum == 4096
    assert campaign.reports[0].optimum_ratio >= 1.0


def test_run_benchmark_variants_can_be_keyed_by_label() -> None:
    few = SyncPatternSource(num_allocations=16, num_threads=2)
    many = SyncPatternSource(num_allocations=16, num_threads=8)

    campaign = _run(
        sources=(few, many), variants={few.label(): 16, "sync_pattern": (24, 32)}
    )

    assert campaign.source_names == tuple(sorted((few.label(), many.label())))
    assert _variants(campaign, few) == {16}
    assert _variants(campaign, many) == {24, 32}


def test_run_benchmark_resolves_fixed_variants_by_name_and_index(
    tmp_path: Path,
) -> None:
    for name in ("a", "b", "c"):
        pool = Pool(id=name, allocations=(Allocation(id=0, size=8, start=0, end=4),))
        save_allocation(pool, tmp_path / f"{name}.csv")
    source = MinimallocSource(csv_dir=tmp_path)

    assert _variants(_run(sources=(source,), variants=("c", 0)), source) == {"a", "c"}
    assert _variants(_run(sources=(source,), variants=2), source) == {"a", "b"}
    with pytest.raises(ValueError, match="Unknown variant 'd'"):
        _run(sources=(source,), variants=("d",))


def test_run_benchmark_tolerates_unmeasurable_pressure() -> None:
    source = SyncPatternSource(
        num_allocations=2000, num_threads=64, pattern="independent"
    )
    report = _run(sources=(source,)).reports[0]

    assert report.num_allocations == 2000
    assert report.mean_seconds > 0
    assert report.mean_allocation_efficiency is None
    assert report.lower_bound is None


@pytest.mark.parametrize(
    ("kwargs", "error", "match"),
    [
        ({"variants": {"randm": 10}}, ValueError, "match no source"),
        ({"variants": ("small",)}, TypeError, "Non-integer variant"),
        ({"iterations": 0}, ValueError, "iterations must be positive"),
    ],
)
def test_run_benchmark_rejects_invalid_arguments(
    kwargs: dict[str, object], error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        _run(**kwargs)
