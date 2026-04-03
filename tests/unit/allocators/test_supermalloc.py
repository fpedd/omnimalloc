#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators import SuperMallocAllocator
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _peak_memory(result: tuple[Allocation, ...]) -> int:
    return max(a.height for a in result if a.height is not None)


def _validate(result: tuple[Allocation, ...]) -> None:
    validate_allocation(Pool(id=0, allocations=result))


def test_empty() -> None:
    allocator = SuperMallocAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


def test_single_allocation() -> None:
    allocator = SuperMallocAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0
    assert result[0].size == 100


def test_two_non_overlapping() -> None:
    allocator = SuperMallocAllocator()
    a = Allocation(id=1, size=100, start=0, end=10)
    b = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((a, b))
    assert result[0].offset == 0
    assert result[1].offset == 0


def test_two_overlapping() -> None:
    allocator = SuperMallocAllocator()
    a = Allocation(id=1, size=100, start=0, end=10)
    b = Allocation(id=2, size=50, start=5, end=15)
    result = allocator.allocate((a, b))
    assert all(r.offset is not None for r in result)
    assert _peak_memory(result) == 150
    _validate(result)


def test_all_overlap() -> None:
    allocator = SuperMallocAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    assert _peak_memory(result) == 500
    _validate(result)


def test_preserves_ids() -> None:
    allocator = SuperMallocAllocator()
    a = Allocation(id="alloc_a", size=100, start=0, end=10)
    b = Allocation(id="alloc_b", size=50, start=5, end=15)
    result = allocator.allocate((a, b))
    assert result[0].id == "alloc_a"
    assert result[1].id == "alloc_b"


def test_partial_overlap_chain() -> None:
    allocator = SuperMallocAllocator()
    a = Allocation(id=1, size=100, start=0, end=10)
    b = Allocation(id=2, size=100, start=5, end=15)
    c = Allocation(id=3, size=100, start=10, end=20)
    d = Allocation(id=4, size=100, start=15, end=25)
    result = allocator.allocate((a, b, c, d))
    assert all(r.offset is not None for r in result)
    # A and C don't overlap temporally, B and D don't overlap temporally
    # Optimal height is 200
    assert _peak_memory(result) == 200
    _validate(result)


def test_no_spatial_temporal_overlap() -> None:
    allocator = SuperMallocAllocator()
    allocs = tuple(
        Allocation(id=i, size=(i + 1) * 10, start=i * 2, end=i * 2 + 5)
        for i in range(8)
    )
    result = allocator.allocate(allocs)
    assert all(r.offset is not None for r in result)
    _validate(result)


def test_deterministic() -> None:
    allocator = SuperMallocAllocator()
    allocs = tuple(
        Allocation(id=i, size=(i % 3 + 1) * 50, start=i * 3, end=i * 3 + 7)
        for i in range(10)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_multiple_partitions() -> None:
    allocator = SuperMallocAllocator()
    # Two disjoint temporal groups
    group1 = [
        Allocation(id=1, size=100, start=0, end=5),
        Allocation(id=2, size=100, start=0, end=5),
    ]
    group2 = [
        Allocation(id=3, size=200, start=10, end=15),
        Allocation(id=4, size=200, start=10, end=15),
    ]
    allocs = tuple(group1 + group2)
    result = allocator.allocate(allocs)
    assert all(r.offset is not None for r in result)
    _validate(result)
    # Each group's peak should be sum of sizes within that group
    # Group 1: 200, Group 2: 400
    # Overall peak depends on offset assignment within each partition
    assert _peak_memory(result) == 400


def test_achieves_lower_bound() -> None:
    allocator = SuperMallocAllocator()
    # Three allocations, all overlap. Lower bound = sum = 60.
    # This is achievable since they must all be stacked.
    a = Allocation(id=1, size=10, start=0, end=10)
    b = Allocation(id=2, size=20, start=0, end=10)
    c = Allocation(id=3, size=30, start=0, end=10)
    result = allocator.allocate((a, b, c))
    assert _peak_memory(result) == 60
    _validate(result)


def test_budget_respected() -> None:
    allocator = SuperMallocAllocator(budget=1)
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    # Even with minimal budget, should return a valid solution
    assert all(r.offset is not None for r in result)
    _validate(result)
