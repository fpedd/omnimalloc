#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.best_fit import BestFitAllocator
from omnimalloc.allocators.greedy import GreedyAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _assert_valid(result: tuple[Allocation, ...]) -> None:
    validate_allocation(Pool(id="test_pool", allocations=result))


def test_best_fit_empty() -> None:
    allocator = BestFitAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


def test_best_fit_single() -> None:
    allocator = BestFitAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0


def test_best_fit_no_temporal_overlap_shares_offset() -> None:
    allocator = BestFitAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    by_id = {a.id: a for a in result}
    assert by_id[1].offset == 0
    assert by_id[2].offset == 0


def test_best_fit_all_overlap_stacks_sequentially() -> None:
    allocator = BestFitAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    _assert_valid(result)
    assert placement_pressure(result) == 500


def test_best_fit_preserves_allocations() -> None:
    allocator = BestFitAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = allocator.allocate(allocs)
    assert len(result) == len(allocs)
    assert {a.id for a in result} == {1, 2}
    assert all(a.offset is not None for a in result)


def test_best_fit_deterministic() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=i % 3, end=i % 3 + i % 7 + 1)
        for i in range(20)
    )
    result1 = BestFitAllocator().allocate(allocs)
    result2 = BestFitAllocator().allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_best_fit_chooses_tighter_gap_than_first_fit() -> None:
    allocs = (
        Allocation(id="a", size=10, start=0, end=100),
        Allocation(id="filler1", size=50, start=1, end=2),
        Allocation(id="b", size=10, start=1, end=55),
        Allocation(id="filler2", size=25, start=3, end=4),
        Allocation(id="c", size=10, start=3, end=55),
        Allocation(id="t", size=15, start=50, end=55),
    )

    first_fit = {a.id: a.offset for a in GreedyAllocator().allocate(allocs)}
    best_fit = {a.id: a.offset for a in BestFitAllocator().allocate(allocs)}

    assert first_fit["t"] == 10
    assert best_fit["t"] == 45
    _assert_valid(BestFitAllocator().allocate(allocs))
    assert placement_pressure(
        tuple(BestFitAllocator().allocate(allocs))
    ) == placement_pressure(tuple(GreedyAllocator().allocate(allocs)))


def test_best_fit_complex_overlap_produces_valid_allocation() -> None:
    allocator = BestFitAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=5),
        Allocation(id=2, size=100, start=3, end=8),
        Allocation(id=3, size=100, start=6, end=10),
        Allocation(id=4, size=50, start=0, end=10),
        Allocation(id=5, size=300, start=2, end=4),
    )
    result = allocator.allocate(allocs)
    _assert_valid(result)
