#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy import (
    GreedyByDurationAllocator,
    GreedyBySizeAllocator,
)
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.sweep import (
    HybridSweepAllocator,
    HybridSweepByAreaAllocator,
    HybridSweepByDurationAllocator,
    HybridSweepBySizeAllocator,
    SweepAllocator,
    SweepBestFitAllocator,
    SweepByAllAllocator,
    SweepTwoEndedAllocator,
)
from omnimalloc.benchmark.sources.generator import RandomSource
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation

ALL_SWEEP_ALLOCATORS = (
    SweepAllocator,
    SweepBestFitAllocator,
    SweepTwoEndedAllocator,
    HybridSweepAllocator,
    HybridSweepBySizeAllocator,
    HybridSweepByDurationAllocator,
    HybridSweepByAreaAllocator,
)


def _is_valid(result: tuple[Allocation, ...]) -> bool:
    return validate_allocation(Pool(id="test_pool", allocations=result))


@pytest.mark.parametrize("allocator_cls", ALL_SWEEP_ALLOCATORS)
def test_sweep_empty(allocator_cls: type) -> None:
    assert allocator_cls().allocate(()) == ()


@pytest.mark.parametrize("allocator_cls", ALL_SWEEP_ALLOCATORS)
def test_sweep_single(allocator_cls: type) -> None:
    result = allocator_cls().allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert len(result) == 1
    assert result[0].offset == 0


def test_sweep_no_temporal_overlap_shares_offset() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=200, start=10, end=20),
    )
    by_id = {a.id: a for a in SweepAllocator().allocate(allocs)}
    assert by_id[1].offset == 0
    assert by_id[2].offset == 0


def test_sweep_all_overlap_stacks_sequentially() -> None:
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = SweepAllocator().allocate(allocs)
    assert _is_valid(result)
    assert peak_memory(result) == 500


def test_sweep_reuses_freed_gap() -> None:
    allocs = (
        Allocation(id="long", size=50, start=0, end=10),
        Allocation(id="early", size=100, start=0, end=5),
        Allocation(id="late", size=100, start=5, end=10),
    )
    by_id = {a.id: a for a in SweepAllocator().allocate(allocs)}
    assert by_id["early"].offset == 0
    assert by_id["long"].offset == 100
    assert by_id["late"].offset == 0


def test_sweep_same_time_places_largest_first() -> None:
    allocs = (
        Allocation(id="small", size=10, start=0, end=10),
        Allocation(id="large", size=100, start=0, end=10),
    )
    by_id = {a.id: a for a in SweepAllocator().allocate(allocs)}
    assert by_id["large"].offset == 0
    assert by_id["small"].offset == 100


def test_sweep_best_fit_picks_tighter_gap() -> None:
    allocs = (
        Allocation(id="pillar_a", size=200, start=0, end=10),
        Allocation(id="hole_wide", size=150, start=0, end=2),
        Allocation(id="pillar_b", size=100, start=0, end=10),
        Allocation(id="hole_tight", size=50, start=0, end=2),
        Allocation(id="pillar_c", size=40, start=0, end=10),
        Allocation(id="probe", size=50, start=3, end=10),
    )
    first = {a.id: a.offset for a in SweepAllocator().allocate(allocs)}
    best = {a.id: a.offset for a in SweepBestFitAllocator().allocate(allocs)}
    assert first["probe"] == 200
    assert best["probe"] == 450


def test_sweep_does_not_mutate_input() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = SweepAllocator().allocate(allocs)
    assert all(a.offset is None for a in allocs)
    assert {a.id for a in result} == {1, 2}
    assert all(a.offset is not None for a in result)


def test_hybrid_sweep_by_size_matches_greedy_by_size_below_budget() -> None:
    allocs = RandomSource(num_allocations=100, seed=7).get_allocations()
    hybrid = HybridSweepBySizeAllocator().allocate(allocs)
    greedy = GreedyBySizeAllocator().allocate(allocs)
    assert peak_memory(hybrid) == peak_memory(greedy)


def test_hybrid_sweep_by_duration_matches_greedy_by_duration_below_budget() -> None:
    allocs = RandomSource(num_allocations=100, seed=7).get_allocations()
    hybrid = HybridSweepByDurationAllocator().allocate(allocs)
    greedy = GreedyByDurationAllocator().allocate(allocs)
    assert peak_memory(hybrid) == peak_memory(greedy)


def test_hybrid_sweep_negative_max_obstacles_raises() -> None:
    with pytest.raises(ValueError, match="max_obstacles"):
        HybridSweepAllocator(max_obstacles=-1)


@pytest.mark.parametrize(
    "allocator_cls", [SweepAllocator, SweepBestFitAllocator, HybridSweepAllocator]
)
def test_sweep_oversized_total_size_raises(allocator_cls: type) -> None:
    with pytest.raises(OverflowError):
        allocator_cls().allocate((Allocation(id=1, size=2**62, start=0, end=1),))


def test_hybrid_sweep_above_budget_is_valid() -> None:
    allocs = RandomSource(num_allocations=200, seed=3).get_allocations()
    result = HybridSweepAllocator(max_obstacles=16).allocate(allocs)
    assert _is_valid(result)
    assert len(result) == len(allocs)


@pytest.mark.parametrize("allocator_cls", ALL_SWEEP_ALLOCATORS)
def test_sweep_deterministic(allocator_cls: type) -> None:
    allocs = RandomSource(num_allocations=50, seed=11).get_allocations()
    result1 = allocator_cls().allocate(allocs)
    result2 = allocator_cls().allocate(allocs)
    by_id1 = {a.id: a.offset for a in result1}
    by_id2 = {a.id: a.offset for a in result2}
    assert by_id1 == by_id2


def test_sweep_by_all_not_worse_than_any_member() -> None:
    allocs = RandomSource(num_allocations=150, seed=5).get_allocations()
    best = peak_memory(SweepByAllAllocator(cores=1).allocate(allocs))
    for allocator_cls in ALL_SWEEP_ALLOCATORS:
        assert best <= peak_memory(allocator_cls().allocate(allocs))


def test_all_sweep_allocators_valid_on_complex_workload() -> None:
    allocs = RandomSource(
        num_allocations=300, duration_max=2000, time_max=4000, seed=13
    ).get_allocations()
    for allocator_cls in ALL_SWEEP_ALLOCATORS:
        result = allocator_cls().allocate(allocs)
        assert _is_valid(result)
        assert len(result) == len(allocs)
