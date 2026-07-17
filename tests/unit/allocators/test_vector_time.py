#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc._cpp import FirstFitPlacer, Partition, conflicts
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.allocators.best_fit import BestFitAllocator
from omnimalloc.allocators.genetic import HAS_DEAP, GeneticAllocator
from omnimalloc.allocators.greedy import GreedyAllocator
from omnimalloc.allocators.greedy_base import (
    order_by_area,
    order_by_conflict,
    order_by_conflict_size,
    order_by_duration,
    order_by_size,
    order_by_start,
)
from omnimalloc.allocators.hillclimb import HillClimbAllocator
from omnimalloc.allocators.minimalloc import HAS_MINIMALLOC, MinimallocAllocator
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.allocators.random import RandomAllocator
from omnimalloc.allocators.simulated_annealing import SimulatedAnnealingAllocator
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.allocators.tabu_search import TabuSearchAllocator
from omnimalloc.allocators.telamalloc import TelamallocAllocator
from omnimalloc.analysis import conflict_degrees, placement_pressure
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation


def vector_problem(n: int = 10) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=i,
            size=32 * (i % 3 + 1),
            start=(i, max(0, i - 2)),
            end=(i + 3, i + 1),
        )
        for i in range(n)
    )


@pytest.mark.parametrize(
    "allocator_cls",
    [
        GreedyAllocator,
        NaiveAllocator,
        OmniAllocator,
        RandomAllocator,
        HillClimbAllocator,
        BestFitAllocator,
    ],
)
def test_allocators_place_vector_problems(
    allocator_cls: type[BaseAllocator],
) -> None:
    allocs = vector_problem()
    result = allocator_cls().allocate(allocs)
    validate_allocation(Pool(id="p", allocations=result))
    assert {a.id for a in result} == {a.id for a in allocs}


@pytest.mark.skipif(not HAS_DEAP, reason="deap not installed")
def test_genetic_places_vector_problems() -> None:
    allocator = GeneticAllocator(population_size=10, max_generations=3)
    result = allocator.allocate(vector_problem())
    validate_allocation(Pool(id="p", allocations=result))


def test_orderings_permute_vector_problems() -> None:
    allocs = vector_problem()
    orders = (
        order_by_size,
        order_by_duration,
        order_by_area,
        order_by_conflict,
        order_by_conflict_size,
        order_by_start,
    )
    for order in orders:
        assert sorted(a.id for a in order(allocs)) == sorted(a.id for a in allocs)


def test_order_by_start_is_lexicographic() -> None:
    allocs = (
        Allocation(id=1, size=1, start=(1, 0), end=(2, 1)),
        Allocation(id=2, size=1, start=(0, 5), end=(1, 6)),
        Allocation(id=3, size=1, start=(0, 2), end=(1, 3)),
    )
    assert [a.id for a in order_by_start(allocs)] == [3, 2, 1]


def test_order_by_start_mixed_dimensions_rejected() -> None:
    mixed = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="dimension"):
        order_by_start(mixed)


def test_conflict_degrees_match_conflict_map() -> None:
    allocs = vector_problem()
    conflict_map = conflicts(allocs, None)
    degrees = conflict_degrees(allocs, work_budget=None)
    for alloc, degree in zip(allocs, degrees, strict=True):
        assert degree == len(conflict_map[alloc.id])


def test_conflict_degrees_scalar_match_conflict_map() -> None:
    allocs = tuple(
        Allocation(id=i, size=8, start=i % 4, end=i % 4 + 2) for i in range(10)
    )
    conflict_map = conflicts(allocs, None)
    degrees = conflict_degrees(allocs, work_budget=None)
    for alloc, degree in zip(allocs, degrees, strict=True):
        assert degree == len(conflict_map[alloc.id])


def test_conflict_degrees_duplicate_ids_match_scalar() -> None:
    scalar = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=1, size=16, start=1, end=5),
        Allocation(id=2, size=8, start=2, end=6),
    )
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    assert conflict_degrees(scalar, work_budget=None) == conflict_degrees(
        lockstep, work_budget=None
    )


def test_conflict_map_matches_pairwise_test() -> None:
    allocs = vector_problem(12)
    conflict_map = conflicts(allocs, None)
    for a in allocs:
        for b in allocs:
            if a.id == b.id:
                continue
            assert (b.id in conflict_map[a.id]) == a.conflicts_with(b)


def test_first_fit_placer_accepts_vector_problems() -> None:
    allocs = vector_problem()
    placer = FirstFitPlacer(list(allocs))
    placed = tuple(placer.place(list(range(len(allocs)))))
    validate_allocation(Pool(id="p", allocations=placed))
    assert placer.peak(list(range(len(allocs)))) == placement_pressure(placed)


def test_order_search_allocators_place_vector_problems() -> None:
    allocs = vector_problem()
    for allocator in (
        SimulatedAnnealingAllocator(max_iterations=20),
        TabuSearchAllocator(max_iterations=20),
    ):
        result = allocator.allocate(allocs)
        validate_allocation(Pool(id="p", allocations=result))
        assert {a.id for a in result} == {a.id for a in allocs}


def test_partition_rejects_vector_time() -> None:
    with pytest.raises(ValueError, match=r"requires scalar .* 2-dim vector clocks"):
        Partition.from_allocations([Allocation(id=1, size=8, start=(0, 1), end=(2, 2))])


def test_supermalloc_rejects_vector_time() -> None:
    with pytest.raises(ValueError, match=r"requires scalar .* 2-dim vector clocks"):
        SupermallocAllocator().allocate(vector_problem())


def test_telamalloc_rejects_vector_time() -> None:
    with pytest.raises(ValueError, match=r"requires scalar .* 2-dim vector clocks"):
        TelamallocAllocator().allocate(vector_problem())


@pytest.mark.skipif(not HAS_MINIMALLOC, reason="minimalloc not installed")
def test_minimalloc_rejects_vector_time() -> None:
    with pytest.raises(ValueError, match=r"requires scalar .* 2-dim vector clocks"):
        MinimallocAllocator().allocate(vector_problem())


def test_registry_declares_vector_time_support() -> None:
    registry = BaseAllocator.registry()
    assert registry["greedy"].supports_vector_time
    assert registry["omni"].supports_vector_time
    assert registry["best_fit"].supports_vector_time
    assert registry["simulated_annealing"].supports_vector_time
    assert registry["tabu_search"].supports_vector_time
    assert not registry["supermalloc"].supports_vector_time
    assert not registry["telamalloc"].supports_vector_time


def test_mixed_dimensions_rejected() -> None:
    mixed = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="dimension"):
        GreedyAllocator().allocate(mixed)


def test_scalar_problems_place_identically_via_vector_path() -> None:
    scalar = tuple(
        Allocation(id=i, size=16 * (i % 4 + 1), start=i % 5, end=i % 5 + i % 3 + 1)
        for i in range(15)
    )
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    scalar_result = GreedyAllocator().allocate(scalar)
    vector_result = GreedyAllocator().allocate(lockstep)
    assert [a.offset for a in scalar_result] == [a.offset for a in vector_result]


def test_reuse_follows_happens_before() -> None:
    ordered = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=100, start=(2, 1), end=(3, 2)),
    )
    assert placement_pressure(GreedyAllocator().allocate(ordered)) == 100

    concurrent = (
        Allocation(id=1, size=100, start=(0, 5), end=(1, 6)),
        Allocation(id=2, size=100, start=(2, 0), end=(3, 1)),
    )
    assert placement_pressure(GreedyAllocator().allocate(concurrent)) == 200
