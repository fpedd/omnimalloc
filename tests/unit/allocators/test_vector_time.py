#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc._cpp import FirstFitPlacer, GreedyOrder, Partition, greedy_order
from omnimalloc.allocators.greedy import (
    GreedyAllocator,
)
from omnimalloc.analysis import placement_pressure
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


@pytest.mark.parametrize("order", list(GreedyOrder))
def test_orders_permute_vector_problems(order: GreedyOrder) -> None:
    allocs = vector_problem()
    assert sorted(greedy_order(allocs, order)) == list(range(len(allocs)))


def test_start_order_is_invariant_under_lane_permutation() -> None:
    allocs = (
        Allocation(id=1, size=1, start=(4, 0), end=(6, 1)),
        Allocation(id=2, size=1, start=(0, 5), end=(1, 7)),
        Allocation(id=3, size=1, start=(1, 1), end=(2, 3)),
    )
    swapped = tuple(
        Allocation(id=a.id, size=a.size, start=a.start[::-1], end=a.end[::-1])
        for a in allocs
    )
    assert greedy_order(allocs, GreedyOrder.START) == greedy_order(
        swapped, GreedyOrder.START
    )


def test_start_order_never_inverts_happens_before() -> None:
    earlier = Allocation(id=1, size=1, start=(0, 0), end=(1, 1))
    later = Allocation(id=2, size=1, start=(1, 1), end=(2, 2))
    assert greedy_order((later, earlier), GreedyOrder.START) == [1, 0]


def test_greedy_order_mixed_dimensions_rejected() -> None:
    mixed = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="dimension"):
        greedy_order(mixed, GreedyOrder.START)


def test_first_fit_placer_accepts_vector_problems() -> None:
    allocs = vector_problem()
    placer = FirstFitPlacer(list(allocs))
    placed = tuple(placer.place(list(range(len(allocs)))))
    validate_allocation(Pool(id="p", allocations=placed))
    assert placer.peak(list(range(len(allocs)))) == placement_pressure(placed)


def test_partition_rejects_vector_time() -> None:
    with pytest.raises(ValueError, match=r"requires scalar .* 2-dim vector clocks"):
        Partition.from_allocations([Allocation(id=1, size=8, start=(0, 1), end=(2, 2))])


def test_mixed_dimensions_rejected() -> None:
    mixed = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="dimension"):
        GreedyAllocator().allocate(mixed)


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
