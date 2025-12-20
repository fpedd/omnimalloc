#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.greedy import GreedyAllocator
from omnimalloc.allocators.greedy_cpp import (
    GreedyAllocatorCpp,
    GreedyByAreaAllocatorCpp,
    GreedyByConflictAllocatorCpp,
    GreedyByDurationAllocatorCpp,
    GreedyBySizeAllocatorCpp,
)
from omnimalloc.primitives import Allocation


def test_greedy_cpp_allocator_empty() -> None:
    allocator = GreedyAllocatorCpp()
    result = allocator.allocate(())
    assert len(result) == 0


def test_greedy_cpp_allocator_single() -> None:
    allocator = GreedyAllocatorCpp()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0
    assert result[0].size == 100


def test_greedy_cpp_allocator_no_temporal_overlap() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 0


def test_greedy_cpp_allocator_temporal_overlap_first_fit() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=50, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_cpp_allocator_gap_reuse() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=200, start=6, end=10)
    alloc3 = Allocation(id=3, size=50, start=6, end=10)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 0
    assert result[2].offset == 200


def test_greedy_cpp_allocator_exact_gap_fit() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=5, end=15)
    alloc3 = Allocation(id=3, size=100, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 200


def test_greedy_cpp_allocator_complex_overlap() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=100, start=3, end=8)
    alloc3 = Allocation(id=3, size=100, start=6, end=10)
    alloc4 = Allocation(id=4, size=50, start=0, end=10)
    result = allocator.allocate((alloc1, alloc2, alloc3, alloc4))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 0
    assert result[3].offset == 200


def test_greedy_cpp_allocator_preserves_ids() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id="alloc_a", size=100, start=0, end=10)
    alloc2 = Allocation(id="alloc_b", size=50, start=5, end=15)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].id == "alloc_a"
    assert result[1].id == "alloc_b"


def test_greedy_cpp_by_duration_empty() -> None:
    allocator = GreedyByDurationAllocatorCpp()
    result = allocator.allocate(())
    assert len(result) == 0


def test_greedy_cpp_by_duration_sorts_by_duration() -> None:
    allocator = GreedyByDurationAllocatorCpp()
    short = Allocation(id=1, size=100, start=0, end=2)
    medium = Allocation(id=2, size=100, start=0, end=5)
    long = Allocation(id=3, size=100, start=0, end=10)
    result = allocator.allocate((short, medium, long))
    assert result[0].id == 3
    assert result[1].id == 2
    assert result[2].id == 1


def test_greedy_cpp_by_duration_allocates_correctly() -> None:
    allocator = GreedyByDurationAllocatorCpp()
    short = Allocation(id=1, size=100, start=0, end=2)
    long = Allocation(id=2, size=100, start=0, end=10)
    result = allocator.allocate((short, long))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_cpp_by_conflict_empty() -> None:
    allocator = GreedyByConflictAllocatorCpp()
    result = allocator.allocate(())
    assert len(result) == 0


def test_greedy_cpp_by_conflict_no_conflicts() -> None:
    allocator = GreedyByConflictAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    assert len(result) == 2
    assert all(a.offset is not None for a in result)


def test_greedy_cpp_by_conflict_sorts_by_conflict_degree() -> None:
    allocator = GreedyByConflictAllocatorCpp()
    low_conflict = Allocation(id=1, size=100, start=0, end=5)
    high_conflict = Allocation(id=2, size=100, start=10, end=20)
    other1 = Allocation(id=3, size=100, start=12, end=18)
    other2 = Allocation(id=4, size=100, start=15, end=25)
    result = allocator.allocate((low_conflict, high_conflict, other1, other2))
    assert result[0].id in [2, 3, 4]
    assert result[3].id == 1


def test_greedy_cpp_by_conflict_uses_size_as_tiebreaker() -> None:
    allocator = GreedyByConflictAllocatorCpp()
    small = Allocation(id=1, size=50, start=0, end=10)
    large = Allocation(id=2, size=200, start=0, end=10)
    result = allocator.allocate((small, large))
    assert result[0].id == 2
    assert result[1].id == 1


def test_greedy_cpp_by_area_empty() -> None:
    allocator = GreedyByAreaAllocatorCpp()
    result = allocator.allocate(())
    assert len(result) == 0


def test_greedy_cpp_by_area_sorts_by_area() -> None:
    allocator = GreedyByAreaAllocatorCpp()
    small_area = Allocation(id=1, size=10, start=0, end=10)
    medium_area = Allocation(id=2, size=100, start=0, end=10)
    large_area = Allocation(id=3, size=100, start=0, end=100)
    result = allocator.allocate((small_area, medium_area, large_area))
    assert result[0].id == 3
    assert result[1].id == 2
    assert result[2].id == 1


def test_greedy_cpp_by_area_allocates_correctly() -> None:
    allocator = GreedyByAreaAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=100)
    alloc2 = Allocation(id=2, size=10, start=50, end=60)
    result = allocator.allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_greedy_cpp_by_size_empty() -> None:
    allocator = GreedyBySizeAllocatorCpp()
    result = allocator.allocate(())
    assert len(result) == 0


def test_greedy_cpp_by_size_sorts_by_size() -> None:
    allocator = GreedyBySizeAllocatorCpp()
    small = Allocation(id=1, size=10, start=0, end=10)
    medium = Allocation(id=2, size=100, start=0, end=10)
    large = Allocation(id=3, size=1000, start=0, end=10)
    result = allocator.allocate((small, medium, large))
    assert result[0].id == 3
    assert result[1].id == 2
    assert result[2].id == 1


def test_greedy_cpp_by_size_allocates_correctly() -> None:
    allocator = GreedyBySizeAllocatorCpp()
    small = Allocation(id=1, size=50, start=0, end=10)
    large = Allocation(id=2, size=200, start=5, end=15)
    result = allocator.allocate((small, large))
    assert result[0].id == 2
    assert result[1].id == 1
    assert result[0].offset == 0
    assert result[1].offset == 200


def test_greedy_cpp_allocator_all_overlap() -> None:
    allocator = GreedyAllocatorCpp()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    offsets = [a.offset for a in result]
    assert offsets == [0, 100, 200, 300, 400]


def test_greedy_cpp_allocator_partial_overlap_chain() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=100, start=5, end=15)
    alloc3 = Allocation(id=3, size=100, start=10, end=20)
    alloc4 = Allocation(id=4, size=100, start=15, end=25)
    result = allocator.allocate((alloc1, alloc2, alloc3, alloc4))
    assert result[0].offset == 0
    assert result[1].offset == 100
    assert result[2].offset == 0
    assert result[3].offset == 100


def test_greedy_cpp_by_duration_deterministic() -> None:
    allocator = GreedyByDurationAllocatorCpp()
    allocs = tuple(
        Allocation(id=i, size=100, start=0, end=i % 5 + 1) for i in range(10)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_greedy_cpp_by_size_deterministic() -> None:
    allocator = GreedyBySizeAllocatorCpp()
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=0, end=10) for i in range(10)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_greedy_cpp_allocator_fits_in_gap() -> None:
    allocator = GreedyAllocatorCpp()
    alloc1 = Allocation(id=1, size=50, start=0, end=5)
    alloc2 = Allocation(id=2, size=50, start=0, end=5)
    alloc3 = Allocation(id=3, size=40, start=0, end=5)
    result = allocator.allocate((alloc1, alloc2, alloc3))
    assert result[0].offset == 0
    assert result[1].offset == 50
    assert result[2].offset == 100


def test_greedy_cpp_matches_python() -> None:
    """Test that C++ implementation produces same results as Python."""
    cpp_allocator = GreedyAllocatorCpp()
    py_allocator = GreedyAllocator()

    # Test with various allocation patterns
    test_cases = [
        tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5)),
        (
            Allocation(id=1, size=100, start=0, end=5),
            Allocation(id=2, size=100, start=3, end=8),
            Allocation(id=3, size=100, start=6, end=10),
            Allocation(id=4, size=50, start=0, end=10),
        ),
        (
            Allocation(id=1, size=50, start=0, end=5),
            Allocation(id=2, size=50, start=0, end=5),
            Allocation(id=3, size=40, start=0, end=5),
        ),
    ]

    for allocs in test_cases:
        cpp_result = cpp_allocator.allocate(allocs)
        py_result = py_allocator.allocate(allocs)
        assert len(cpp_result) == len(py_result)
        for cpp_alloc, py_alloc in zip(cpp_result, py_result, strict=True):
            assert cpp_alloc.offset == py_alloc.offset
            assert cpp_alloc.id == py_alloc.id
