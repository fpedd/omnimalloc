#
# SPDX-License-Identifier: Apache-2.0
#

import threading

import pytest
from omnimalloc._cpp import FirstFitPlacer, first_fit_place
from omnimalloc.allocators import greedy
from omnimalloc.allocators.greedy import (
    GreedyAllocator,
    GreedyByAllAllocator,
    GreedyByAreaAllocator,
    GreedyByConflictAllocator,
    GreedyByConflictSizeAllocator,
    GreedyByDurationAllocator,
    GreedyBySizeAllocator,
    GreedyByStartAllocator,
    allocate_parallel,
)
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation
from omnimalloc.validate import validate_allocation


@pytest.mark.parametrize(
    "allocator_cls",
    [
        GreedyAllocator,
        GreedyByDurationAllocator,
        GreedyByConflictAllocator,
        GreedyByAreaAllocator,
        GreedyBySizeAllocator,
        GreedyByConflictSizeAllocator,
        GreedyByStartAllocator,
        GreedyByAllAllocator,
    ],
)
def test_greedy_allocator_empty(allocator_cls: type[GreedyAllocator]) -> None:
    result = allocator_cls().allocate(())
    assert len(result) == 0


def test_greedy_allocator_single() -> None:
    allocator = GreedyAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0
    assert result[0].size == 100


def test_greedy_allocator_no_temporal_overlap() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 0


def test_greedy_allocator_temporal_overlap_first_fit() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=50, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_allocator_gap_reuse() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=200, start=6, end=10)
    alloc3 = Allocation(id=3, size=50, start=6, end=10)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 0
    assert result[2].offset == 200


def test_greedy_allocator_exact_gap_fit() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=5, end=15)
    alloc3 = Allocation(id=3, size=100, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 200


def test_greedy_allocator_complex_overlap() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=100, start=3, end=8)
    alloc3 = Allocation(id=3, size=100, start=6, end=10)
    alloc4 = Allocation(id=4, size=50, start=0, end=10)
    result = allocator.allocate((alloc1, alloc2, alloc3, alloc4))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 0
    assert result[3].offset == 200


def test_greedy_allocator_preserves_ids() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id="alloc_a", size=100, start=0, end=10)
    alloc2 = Allocation(id="alloc_b", size=50, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].id == "alloc_a"
    assert result[1].id == "alloc_b"


def test_greedy_by_duration_sorts_by_duration() -> None:
    allocator = GreedyByDurationAllocator()
    short = Allocation(id=1, size=100, start=0, end=2)
    medium = Allocation(id=2, size=100, start=0, end=5)
    long = Allocation(id=3, size=100, start=0, end=10)
    result = allocator.allocate((short, medium, long))
    assert result[0].id == 3
    assert result[1].id == 2
    assert result[2].id == 1


def test_greedy_by_duration_allocates_correctly() -> None:
    allocator = GreedyByDurationAllocator()
    short = Allocation(id=1, size=100, start=0, end=2)
    long = Allocation(id=2, size=100, start=0, end=10)
    result = allocator.allocate((short, long))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_by_conflict_no_conflicts() -> None:
    allocator = GreedyByConflictAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    assert len(result) == 2
    assert all(a.offset is not None for a in result)


def test_greedy_by_conflict_sorts_by_conflict_degree() -> None:
    allocator = GreedyByConflictAllocator()
    low_conflict = Allocation(id=1, size=100, start=0, end=5)
    high_conflict = Allocation(id=2, size=100, start=10, end=20)
    other1 = Allocation(id=3, size=100, start=12, end=18)
    other2 = Allocation(id=4, size=100, start=15, end=25)
    result = allocator.allocate((low_conflict, high_conflict, other1, other2))
    assert result[0].id in [2, 3, 4]
    assert result[3].id == 1


def test_greedy_by_conflict_uses_size_as_tiebreaker() -> None:
    allocator = GreedyByConflictAllocator()
    small = Allocation(id=1, size=50, start=0, end=10)
    large = Allocation(id=2, size=200, start=0, end=10)
    result = allocator.allocate((small, large))
    assert result[0].id == 2
    assert result[1].id == 1


def test_greedy_by_area_sorts_by_area() -> None:
    allocator = GreedyByAreaAllocator()
    small_area = Allocation(id=1, size=10, start=0, end=10)
    medium_area = Allocation(id=2, size=100, start=0, end=10)
    large_area = Allocation(id=3, size=100, start=0, end=100)
    result = allocator.allocate((small_area, medium_area, large_area))
    assert result[0].id == 3
    assert result[1].id == 2
    assert result[2].id == 1


def test_greedy_by_area_allocates_correctly() -> None:
    allocator = GreedyByAreaAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=100)
    alloc2 = Allocation(id=2, size=10, start=50, end=60)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_by_size_sorts_by_size() -> None:
    allocator = GreedyBySizeAllocator()
    small = Allocation(id=1, size=10, start=0, end=10)
    medium = Allocation(id=2, size=100, start=0, end=10)
    large = Allocation(id=3, size=1000, start=0, end=10)
    result = allocator.allocate((small, medium, large))
    assert result[0].id == 3
    assert result[1].id == 2
    assert result[2].id == 1


def test_greedy_by_size_allocates_correctly() -> None:
    allocator = GreedyBySizeAllocator()
    small = Allocation(id=1, size=50, start=0, end=10)
    large = Allocation(id=2, size=200, start=5, end=15)
    result = allocator.allocate((small, large))
    assert result[0].id == 2
    assert result[1].id == 1
    assert result[0].offset == 0
    assert result[1].offset == 200


def test_greedy_by_conflict_size_sorts_by_product() -> None:
    allocator = GreedyByConflictSizeAllocator()
    big_lonely = Allocation(id=1, size=1000, start=0, end=5)
    busy_large = Allocation(id=2, size=100, start=10, end=20)
    busy_medium = Allocation(id=3, size=50, start=10, end=20)
    busy_small = Allocation(id=4, size=20, start=10, end=20)
    result = allocator.allocate((big_lonely, busy_large, busy_medium, busy_small))
    assert [a.id for a in result] == [2, 3, 4, 1]


def test_greedy_by_conflict_size_uses_size_as_tiebreaker() -> None:
    allocator = GreedyByConflictSizeAllocator()
    small = Allocation(id=1, size=50, start=0, end=10)
    large = Allocation(id=2, size=200, start=20, end=30)
    result = allocator.allocate((small, large))
    assert result[0].id == 2
    assert result[1].id == 1


def test_greedy_by_start_sorts_by_start() -> None:
    allocator = GreedyByStartAllocator()
    late = Allocation(id=1, size=100, start=20, end=30)
    early = Allocation(id=2, size=100, start=0, end=10)
    middle = Allocation(id=3, size=100, start=10, end=20)
    result = allocator.allocate((late, early, middle))
    assert [a.id for a in result] == [2, 3, 1]


def test_greedy_by_start_uses_size_as_tiebreaker() -> None:
    allocator = GreedyByStartAllocator()
    small = Allocation(id=1, size=50, start=0, end=10)
    large = Allocation(id=2, size=200, start=0, end=10)
    result = allocator.allocate((small, large))
    assert result[0].id == 2
    assert result[1].id == 1


def test_greedy_by_start_allocates_correctly() -> None:
    allocator = GreedyByStartAllocator()
    later = Allocation(id=1, size=50, start=5, end=15)
    earlier = Allocation(id=2, size=100, start=0, end=10)
    result = allocator.allocate((later, earlier))
    assert result[0].id == 2
    assert result[1].id == 1
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_allocator_all_overlap() -> None:
    allocator = GreedyAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    offsets = [a.offset for a in result]
    assert offsets == [0, 100, 200, 300, 400]


def test_greedy_allocator_partial_overlap_chain() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=5, end=15)
    alloc3 = Allocation(id=3, size=100, start=10, end=20)
    alloc4 = Allocation(id=4, size=100, start=15, end=25)
    result = allocator.allocate((alloc1, alloc2, alloc3, alloc4))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 0
    assert result[3].offset == 100


@pytest.mark.parametrize(
    "allocator_cls",
    [GreedyByDurationAllocator, GreedyBySizeAllocator, GreedyByAllAllocator],
)
def test_greedy_deterministic(allocator_cls: type[GreedyAllocator]) -> None:
    allocator = allocator_cls()
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=0, end=i % 7 + 1)
        for i in range(20)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_greedy_allocator_fits_in_gap() -> None:
    allocator = GreedyAllocator()
    alloc1 = Allocation(id=1, size=50, start=0, end=5)
    alloc2 = Allocation(id=2, size=50, start=0, end=5)
    alloc3 = Allocation(id=3, size=40, start=0, end=5)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 50
    assert result[2].offset == 100


def test_greedy_by_all_preserves_allocations() -> None:
    allocator = GreedyByAllAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = allocator.allocate(allocs)
    assert len(result) == len(allocs)
    assert {a.id for a in result} == {1, 2}
    assert all(a.offset is not None for a in result)


def test_greedy_by_all_picks_best_peak() -> None:
    allocator = GreedyByAllAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=5),
        Allocation(id=2, size=100, start=3, end=8),
        Allocation(id=3, size=100, start=6, end=10),
        Allocation(id=4, size=50, start=0, end=10),
        Allocation(id=5, size=300, start=2, end=4),
    )
    result = allocator.allocate(allocs)
    peak = placement_pressure(result)

    variants = (
        GreedyAllocator(),
        GreedyBySizeAllocator(),
        GreedyByDurationAllocator(),
        GreedyByAreaAllocator(),
        GreedyByConflictAllocator(),
        GreedyByConflictSizeAllocator(),
        GreedyByStartAllocator(),
    )
    best_variant_peak = min(placement_pressure(v.allocate(allocs)) for v in variants)
    assert peak == best_variant_peak


def test_allocate_parallel_empty() -> None:
    result = allocate_parallel((), (GreedyAllocator(),), num_threads=2)
    assert result == ()


def test_allocate_parallel_matches_serial() -> None:
    variants = (
        GreedyAllocator(),
        GreedyBySizeAllocator(),
        GreedyByDurationAllocator(),
        GreedyByConflictSizeAllocator(),
        GreedyByStartAllocator(),
    )
    allocs = tuple(
        Allocation(
            id=f"alloc_{i}",
            size=(i % 5 + 1) * 10,
            start=(i * 3) % 17,
            end=(i * 3) % 17 + i % 4 + 1,
        )
        for i in range(30)
    )
    serial = allocate_parallel(allocs, variants, num_threads=1)
    parallel = allocate_parallel(allocs, variants, num_threads=2)
    assert {a.id: a.offset for a in parallel} == {a.id: a.offset for a in serial}


def test_allocate_parallel_matches_serial_order() -> None:
    allocs = (
        Allocation(id="b", size=10, start=0, end=5),
        Allocation(id="a", size=20, start=3, end=8),
    )
    serial = allocate_parallel(allocs, (GreedyBySizeAllocator(),), num_threads=1)
    parallel = allocate_parallel(allocs, (GreedyBySizeAllocator(),), num_threads=2)
    assert [a.id for a in parallel] == [a.id for a in serial] == ["a", "b"]


def test_allocate_parallel_tie_break_takes_first_variant() -> None:
    allocs = (
        Allocation(id="a", size=10, start=0, end=5),
        Allocation(id="b", size=20, start=0, end=5),
    )
    variants = (GreedyAllocator(), GreedyBySizeAllocator())
    for result in (
        allocate_parallel(allocs, variants, num_threads=1),
        allocate_parallel(allocs, variants, num_threads=2),
    ):
        assert {a.id: a.offset for a in result} == {"a": 0, "b": 10}
    flipped = allocate_parallel(allocs, variants[::-1], num_threads=2)
    assert {a.id: a.offset for a in flipped} == {"a": 20, "b": 0}


def test_allocate_parallel_accepts_configured_variant() -> None:
    allocs = (
        Allocation(id="a", size=10, start=0, end=5),
        Allocation(id="b", size=20, start=3, end=8),
    )
    variants = (GreedyBySizeAllocator(), GreedyByAllAllocator(num_threads=1))
    result = allocate_parallel(allocs, variants, num_threads=2)
    assert placement_pressure(result) == 30


def test_allocate_parallel_rejects_non_positive_num_threads() -> None:
    allocs = (Allocation(id="a", size=10, start=0, end=5),)
    with pytest.raises(ValueError, match="num_threads must be positive or None"):
        allocate_parallel(allocs, (GreedyAllocator(),), num_threads=0)


def test_greedy_by_all_rejects_non_positive_num_threads() -> None:
    with pytest.raises(ValueError, match="num_threads must be positive or None"):
        GreedyByAllAllocator(num_threads=0)


def test_greedy_by_all_default_matches_single_core() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 4 + 1) * 50, start=i % 5, end=i % 5 + i % 3 + 1)
        for i in range(20)
    )
    default = GreedyByAllAllocator().allocate(allocs)
    single = GreedyByAllAllocator(num_threads=1).allocate(allocs)
    assert {a.id: a.offset for a in default} == {a.id: a.offset for a in single}


def test_allocate_parallel_serial_when_single_core() -> None:
    allocs = (Allocation(id="a", size=10, start=0, end=5),)
    variants = (GreedyAllocator(), GreedyBySizeAllocator())
    result = allocate_parallel(allocs, variants, num_threads=1)
    assert {a.id: a.offset for a in result} == {"a": 0}


def test_greedy_by_all_parallel_matches_serial() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=i % 6, end=i % 6 + i % 7 + 1)
        for i in range(25)
    )
    serial = GreedyByAllAllocator(num_threads=1).allocate(allocs)
    parallel = GreedyByAllAllocator(num_threads=2).allocate(allocs)
    assert {a.id: a.offset for a in parallel} == {a.id: a.offset for a in serial}


def test_allocate_parallel_configured_variant_keeps_kwargs() -> None:
    from omnimalloc.allocators.simulated_annealing import SimulatedAnnealingAllocator

    allocs = (
        Allocation(id="a", size=10, start=0, end=5),
        Allocation(id="b", size=20, start=3, end=8),
    )
    variant = SimulatedAnnealingAllocator(seed=123, max_iterations=5)
    serial = allocate_parallel(allocs, (variant,), num_threads=1)
    parallel = allocate_parallel(allocs, (variant, variant), num_threads=2)
    assert {a.id: a.offset for a in parallel} == {a.id: a.offset for a in serial}


def test_first_fit_placer_rejects_out_of_range_order() -> None:
    placer = FirstFitPlacer([Allocation(id=1, size=10, start=0, end=5)])
    with pytest.raises(ValueError, match="out of range"):
        placer.place([1])


def test_first_fit_placer_rejects_repeated_order_index() -> None:
    placer = FirstFitPlacer(
        [
            Allocation(id=1, size=10, start=0, end=5),
            Allocation(id=2, size=20, start=0, end=5),
        ]
    )
    with pytest.raises(ValueError, match="more than once"):
        placer.peak([0, 0])


def test_first_fit_place_matches_greedy_across_orders() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 4 + 1) * 10, start=i % 5, end=i % 5 + i % 3 + 1)
        for i in range(30)
    )
    for order in (allocs, tuple(reversed(allocs))):
        result = first_fit_place(order)
        expected = GreedyAllocator().allocate(order)
        assert [(a.id, a.offset) for a in result] == [
            (a.id, a.offset) for a in expected
        ]


def test_conflict_orders_refuse_a_vector_instance_over_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(greedy, "DEFAULT_WORK_BUDGET", 0)
    allocations = tuple(
        Allocation(id=i, size=8, start=(i, 0), end=(i + 2, 1)) for i in range(20)
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        GreedyByConflictAllocator().allocate(allocations)
    with pytest.raises(RuntimeError, match="work_budget"):
        GreedyByConflictSizeAllocator().allocate(allocations)


def test_greedy_by_all_still_places_when_the_conflict_orders_refuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(greedy, "DEFAULT_WORK_BUDGET", 0)
    allocations = tuple(
        Allocation(id=i, size=8, start=(i, 0), end=(i + 2, 1)) for i in range(20)
    )
    placed = GreedyByAllAllocator(num_threads=1).allocate(allocations)
    validate_allocation(placed)
    assert {a.id for a in placed} == {a.id for a in allocations}


def test_scalar_conflict_orders_ignore_the_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(greedy, "DEFAULT_WORK_BUDGET", 0)
    allocations = tuple(Allocation(id=i, size=8, start=i, end=i + 2) for i in range(20))
    validate_allocation(GreedyByConflictAllocator().allocate(allocations))


def test_greedy_by_all_from_a_threaded_caller_places_every_allocation() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=i, end=i + 3) for i in range(64))
    results: list[object] = []

    def run() -> None:
        try:
            results.append(GreedyByAllAllocator().allocate(allocations))
        except Exception as e:  # noqa: BLE001
            results.append(e)

    threads = [threading.Thread(target=run) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not [r for r in results if isinstance(r, Exception)]
    for placed in results:
        validate_allocation(placed)
