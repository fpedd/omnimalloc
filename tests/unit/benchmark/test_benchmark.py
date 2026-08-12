#
# SPDX-License-Identifier: Apache-2.0
#


import inspect

import pytest
from omnimalloc.allocators import GreedyAllocator, NaiveAllocator
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.benchmark.benchmark import run_benchmark
from omnimalloc.benchmark.sources.concurrent_tiling import ConcurrentTilingSource
from omnimalloc.benchmark.sources.generator import RandomSource
from omnimalloc.benchmark.sources.pinwheel import PinwheelSource
from omnimalloc.benchmark.sources.sync_patterns import SyncPatternSource
from omnimalloc.benchmark.sources.tiling import TilingSource


def test_run_benchmark_basic() -> None:
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


def test_run_benchmark_per_source_variants() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocator = GreedyAllocator()

    campaign = run_benchmark(
        allocators=(allocator,),
        sources=(source,),
        iterations=1,
        variants={"random": (5, 10)},
    )

    assert campaign.num_reports == 2
    assert {r.variant_id for r in campaign.reports} == {5, 10}


def test_run_benchmark_on_vector_clock_source() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(source,),
        iterations=1,
        variants=16,
        validate=True,
    )

    assert campaign.num_reports == 1
    assert campaign.reports[0].mean_allocation_efficiency > 0


def test_run_benchmark_skips_scalar_only_allocators_on_vector_source() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    campaign = run_benchmark(
        allocators=(SupermallocAllocator(), GreedyAllocator()),
        sources=(source,),
        iterations=1,
        variants=16,
    )

    assert campaign.num_reports == 1
    assert campaign.reports[0].allocator_name == "greedy"


def test_run_benchmark_skips_unsupported_variants() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=4, num_syncs=8)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(source,),
        iterations=1,
        variants=(2, 16),
    )

    assert campaign.num_reports == 1
    assert campaign.reports[0].variant_id == 16


def test_run_benchmark_raises_when_all_pairs_skipped() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    with pytest.raises(ValueError, match="No benchmark reports"):
        run_benchmark(
            allocators=(SupermallocAllocator(),),
            sources=(source,),
            iterations=1,
            variants=16,
        )


def test_run_benchmark_validates_by_default() -> None:
    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(RandomSource(num_allocations=10, seed=42),),
        iterations=1,
        variants=10,
    )

    assert inspect.signature(run_benchmark).parameters["validate"].default is True
    assert campaign.num_reports == 1


def test_run_benchmark_records_skipped_allocators_in_metadata() -> None:
    source = ConcurrentTilingSource(num_allocations=16, num_threads=2, num_syncs=8)

    campaign = run_benchmark(
        allocators=(SupermallocAllocator(), GreedyAllocator()),
        sources=(source,),
        iterations=1,
        variants=(16, 32),
    )

    skipped = campaign.metadata["skipped_allocators"]
    assert skipped == [
        {
            "source": source.label(),
            "allocator": "supermalloc",
            "reason": (
                "supermalloc requires scalar (interval) lifetimes, "
                "got 2-dim vector clocks"
            ),
        }
    ]


def test_run_benchmark_metadata_lists_no_skips_when_all_supported() -> None:
    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(RandomSource(num_allocations=10, seed=42),),
        iterations=1,
        variants=10,
    )

    assert campaign.metadata["skipped_allocators"] == []
    assert campaign.metadata["skipped_variants"] == []


def test_run_benchmark_records_unreachable_variants_in_metadata() -> None:
    source = PinwheelSource(num_allocations=65)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(source,),
        iterations=1,
        variants=(64, 65),
    )

    assert campaign.num_reports == 1
    assert campaign.metadata["skipped_variants"] == [
        {
            "source": source.label(),
            "variant": "64",
            "reason": "cannot reach exactly 64 allocations, nearest is 65",
        }
    ]


def test_run_benchmark_reports_an_unreachable_variant_once_per_source() -> None:
    campaign = run_benchmark(
        allocators=(GreedyAllocator(), NaiveAllocator()),
        sources=(PinwheelSource(num_allocations=65),),
        iterations=1,
        variants=(64, 65),
    )

    assert len(campaign.metadata["skipped_variants"]) == 1


def test_run_benchmark_records_known_optimum_when_available() -> None:
    capacity = 1024 * 1024
    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(TilingSource(num_allocations=32, capacity=capacity),),
        iterations=1,
        variants=32,
    )

    report = campaign.reports[0]
    assert report.known_optimum == capacity
    assert report.optimum_ratio is not None
    assert report.optimum_ratio >= 1.0


def test_run_benchmark_leaves_known_optimum_empty_without_ground_truth() -> None:
    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(RandomSource(num_allocations=10, seed=42),),
        iterations=1,
        variants=10,
    )

    assert campaign.reports[0].known_optimum is None
    assert campaign.reports[0].optimum_ratio is None


def test_run_benchmark_keeps_thread_counts_as_separate_series() -> None:
    few = SyncPatternSource(num_allocations=16, num_threads=2)
    many = SyncPatternSource(num_allocations=16, num_threads=8)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(few, many),
        iterations=1,
        variants=16,
    )

    assert campaign.num_sources == 2
    assert campaign.source_names == tuple(sorted((few.label(), many.label())))


def test_run_benchmark_variants_can_be_keyed_by_label() -> None:
    few = SyncPatternSource(num_allocations=16, num_threads=2)
    many = SyncPatternSource(num_allocations=16, num_threads=8)

    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(few, many),
        iterations=1,
        variants={few.label(): 16, "sync_pattern": (24, 32)},
    )

    by_source = campaign.reports_by_source_allocator_variant
    assert set(by_source[few.label()]["greedy"]) == {"16"}
    assert set(by_source[many.label()]["greedy"]) == {"24", "32"}


def test_run_benchmark_tolerates_unmeasurable_pressure() -> None:
    campaign = run_benchmark(
        allocators=(GreedyAllocator(),),
        sources=(
            SyncPatternSource(
                num_allocations=2000, num_threads=64, pattern="independent"
            ),
        ),
        iterations=1,
        variants=2000,
    )

    report = campaign.reports[0]
    assert report.num_allocations == 2000
    assert report.mean_seconds > 0
    assert report.mean_allocation_efficiency is None
    assert report.lower_bound is None


def test_run_benchmark_defaults_to_source_configured_size() -> None:
    source = RandomSource(num_allocations=12, seed=42)
    campaign = run_benchmark(
        allocators=(GreedyAllocator(),), sources=(source,), iterations=1
    )
    assert campaign.reports[0].num_allocations == 12


def test_run_benchmark_rejects_unknown_variants_key() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    with pytest.raises(ValueError, match="match no source"):
        run_benchmark(
            allocators=(GreedyAllocator(),), sources=(source,), variants={"randm": 10}
        )


def test_run_benchmark_rejects_non_integer_variant_for_parameterizable_source() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    with pytest.raises(TypeError, match="Non-integer variant"):
        run_benchmark(
            allocators=(GreedyAllocator(),), sources=(source,), variants=("small",)
        )


def test_run_benchmark_rejects_non_positive_iterations() -> None:
    with pytest.raises(ValueError, match="iterations must be positive"):
        run_benchmark(
            allocators=(GreedyAllocator(),),
            sources=(RandomSource(num_allocations=10, seed=42),),
            iterations=0,
        )
