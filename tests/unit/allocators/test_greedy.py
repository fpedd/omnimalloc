#
# SPDX-License-Identifier: Apache-2.0
#

import threading

import pytest
from omnimalloc._cpp import FirstFitPlacer, GreedyOrder, greedy_order, greedy_place
from omnimalloc.allocators.greedy import (
    GreedyAllocator,
    GreedyByAllAllocator,
    GreedyByAreaAllocator,
    GreedyByConflictAllocator,
    GreedyByConflictSizeAllocator,
    GreedyByDurationAllocator,
    GreedyBySizeAllocator,
    GreedyByStartAllocator,
)
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation
from omnimalloc.validate import validate_allocation

SINGLE_ORDERS = (
    GreedyAllocator,
    GreedyBySizeAllocator,
    GreedyByDurationAllocator,
    GreedyByAreaAllocator,
    GreedyByConflictAllocator,
    GreedyByConflictSizeAllocator,
    GreedyByStartAllocator,
)


def _alloc(i: int, size: int, start: int, end: int) -> Allocation:
    return Allocation(id=i, size=size, start=start, end=end)


@pytest.mark.parametrize(
    ("allocations", "offsets"),
    [
        ([(100, 0, 10)], [0]),
        ([(100, 0, 10), (200, 10, 20)], [0, 0]),
        ([(100, 0, 10), (50, 5, 15)], [0, 100]),
        ([(100, 0, 5), (200, 6, 10), (50, 6, 10)], [0, 0, 200]),
        ([(100, 0, 10), (100, 5, 15), (100, 5, 15)], [0, 100, 200]),
        ([(100, 0, 5), (100, 3, 8), (100, 6, 10), (50, 0, 10)], [0, 100, 0, 200]),
        ([(100, 0, 10)] * 5, [0, 100, 200, 300, 400]),
        ([(100, 0, 10), (100, 5, 15), (100, 10, 20), (100, 15, 25)], [0, 100, 0, 100]),
        ([(50, 0, 5), (50, 0, 5), (40, 0, 5)], [0, 50, 100]),
    ],
)
def test_greedy_allocator_places_first_fit_in_input_order(
    allocations: list[tuple[int, int, int]], offsets: list[int]
) -> None:
    allocs = tuple(_alloc(i, *spec) for i, spec in enumerate(allocations))
    assert [a.offset for a in GreedyAllocator().allocate(allocs)] == offsets


@pytest.mark.parametrize(
    ("order", "allocations", "expected"),
    [
        (GreedyOrder.INPUT, [(10, 0, 5), (20, 0, 5)], [0, 1]),
        (GreedyOrder.SIZE, [(10, 0, 10), (100, 0, 10), (1000, 0, 10)], [2, 1, 0]),
        (GreedyOrder.SIZE, [(10, 0, 5), (10, 3, 8)], [0, 1]),
        (GreedyOrder.DURATION, [(100, 0, 2), (100, 0, 5), (100, 0, 10)], [2, 1, 0]),
        (GreedyOrder.AREA, [(10, 0, 10), (100, 0, 10), (100, 0, 100)], [2, 1, 0]),
        (
            GreedyOrder.CONFLICT,
            [(100, 0, 5), (100, 10, 20), (100, 12, 18), (100, 15, 25)],
            [1, 2, 3, 0],
        ),
        (GreedyOrder.CONFLICT, [(50, 0, 10), (200, 0, 10)], [1, 0]),
        (
            GreedyOrder.CONFLICT_SIZE,
            [(1000, 0, 5), (100, 10, 20), (50, 10, 20), (20, 10, 20)],
            [1, 2, 3, 0],
        ),
        (GreedyOrder.CONFLICT_SIZE, [(50, 0, 10), (200, 20, 30)], [1, 0]),
        (GreedyOrder.START, [(100, 20, 30), (100, 0, 10), (100, 10, 20)], [1, 2, 0]),
        (GreedyOrder.START, [(50, 0, 10), (200, 0, 10)], [1, 0]),
        (GreedyOrder.START, [(50, 0, 10), (50, 0, 10)], [0, 1]),
    ],
)
def test_greedy_order(
    order: GreedyOrder, allocations: list[tuple[int, int, int]], expected: list[int]
) -> None:
    allocs = [_alloc(i, *spec) for i, spec in enumerate(allocations)]
    assert greedy_order(allocs, order) == expected


@pytest.mark.parametrize(
    ("allocator_cls", "allocations", "offsets"),
    [
        (GreedyByDurationAllocator, [(100, 0, 2), (100, 0, 10)], [100, 0]),
        (GreedyByAreaAllocator, [(100, 0, 100), (10, 50, 60)], [0, 100]),
        (GreedyBySizeAllocator, [(50, 0, 10), (200, 5, 15)], [200, 0]),
        (GreedyByStartAllocator, [(50, 5, 15), (100, 0, 10)], [100, 0]),
    ],
)
def test_greedy_allocator_places_in_its_order(
    allocator_cls: type[GreedyAllocator],
    allocations: list[tuple[int, int, int]],
    offsets: list[int],
) -> None:
    allocs = tuple(_alloc(i, *spec) for i, spec in enumerate(allocations))
    assert [a.offset for a in allocator_cls().allocate(allocs)] == offsets


def test_greedy_by_all_picks_best_peak() -> None:
    allocs = (
        _alloc(1, 100, 0, 5),
        _alloc(2, 100, 3, 8),
        _alloc(3, 100, 6, 10),
        _alloc(4, 50, 0, 10),
        _alloc(5, 300, 2, 4),
    )
    peak = placement_pressure(GreedyByAllAllocator().allocate(allocs))
    assert peak == min(
        placement_pressure(cls().allocate(allocs)) for cls in SINGLE_ORDERS
    )


def test_greedy_by_all_from_a_threaded_caller_places_every_allocation() -> None:
    allocations = tuple(_alloc(i, 8, i, i + 3) for i in range(64))
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


def test_first_fit_placer_rejects_out_of_range_order() -> None:
    placer = FirstFitPlacer([_alloc(1, 10, 0, 5)])
    with pytest.raises(ValueError, match="out of range"):
        placer.place([1])


def test_first_fit_placer_rejects_repeated_order_index() -> None:
    placer = FirstFitPlacer([_alloc(1, 10, 0, 5), _alloc(2, 20, 0, 5)])
    with pytest.raises(ValueError, match="more than once"):
        placer.peak([0, 0])


@pytest.mark.parametrize("order", list(GreedyOrder))
def test_first_fit_placer_matches_greedy_place(order: GreedyOrder) -> None:
    allocs = [_alloc(i, (i % 4 + 1) * 10, i % 5, i % 5 + i % 3 + 1) for i in range(30)]
    placer = FirstFitPlacer(allocs)
    permutation = greedy_order(allocs, order)
    expected = {a.id: a.offset for a in greedy_place(allocs, order)}
    assert {a.id: a.offset for a in placer.place(permutation)} == expected
    assert placer.peak(permutation) == max(a.height for a in placer.place(permutation))
