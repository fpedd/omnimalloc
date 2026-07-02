#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.greedy import (
    GreedyAllocator,
    GreedyByAllAllocator,
    GreedyByAreaAllocator,
    GreedyByConflictAllocator,
    GreedyByConflictSizeAllocator,
    GreedyByDurationAllocator,
    GreedyBySizeAllocator,
    GreedyByStartAllocator,
    compute_conflict_degrees,
)
from omnimalloc.primitives import Allocation


def test_greedy_allocator_empty() -> None:
    allocator = GreedyAllocator()
    result = allocator.allocate(())
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


def test_greedy_by_duration_empty() -> None:
    allocator = GreedyByDurationAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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


def test_compute_conflict_degrees_empty() -> None:
    assert compute_conflict_degrees(()) == {}


def test_compute_conflict_degrees_counts_overlaps() -> None:
    alone = Allocation(id=1, size=10, start=0, end=5)
    pair_a = Allocation(id=2, size=10, start=10, end=20)
    pair_b = Allocation(id=3, size=10, start=15, end=25)
    degrees = compute_conflict_degrees((alone, pair_a, pair_b))
    assert degrees[alone] == 0
    assert degrees[pair_a] == 1
    assert degrees[pair_b] == 1


def test_compute_conflict_degrees_touching_intervals_do_not_conflict() -> None:
    first = Allocation(id=1, size=10, start=0, end=10)
    second = Allocation(id=2, size=10, start=10, end=20)
    degrees = compute_conflict_degrees((first, second))
    assert degrees[first] == 0
    assert degrees[second] == 0


def test_compute_conflict_degrees_matches_bruteforce() -> None:
    allocs = tuple(
        Allocation(id=i, size=i + 1, start=(i * 7) % 23, end=(i * 7) % 23 + i % 6 + 1)
        for i in range(50)
    )
    degrees = compute_conflict_degrees(allocs)
    for alloc in allocs:
        expected = sum(
            1 for other in allocs if other != alloc and alloc.overlaps_temporally(other)
        )
        assert degrees[alloc] == expected


def test_greedy_by_conflict_empty() -> None:
    allocator = GreedyByConflictAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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


def test_greedy_by_area_empty() -> None:
    allocator = GreedyByAreaAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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


def test_greedy_by_size_empty() -> None:
    allocator = GreedyBySizeAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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


def test_greedy_by_conflict_size_empty() -> None:
    allocator = GreedyByConflictSizeAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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


def test_greedy_by_start_empty() -> None:
    allocator = GreedyByStartAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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


def test_greedy_by_duration_deterministic() -> None:
    allocator = GreedyByDurationAllocator()
    allocs = tuple(
        Allocation(id=i, size=100, start=0, end=i % 5 + 1) for i in range(10)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_greedy_by_size_deterministic() -> None:
    allocator = GreedyBySizeAllocator()
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=0, end=10) for i in range(10)
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


def test_greedy_by_all_empty() -> None:
    allocator = GreedyByAllAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


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
    peak = max(a.height for a in result if a.height is not None)

    variants = (
        GreedyAllocator(),
        GreedyBySizeAllocator(),
        GreedyByDurationAllocator(),
        GreedyByAreaAllocator(),
        GreedyByConflictAllocator(),
        GreedyByConflictSizeAllocator(),
        GreedyByStartAllocator(),
    )
    best_variant_peak = min(
        max(a.height for a in v.allocate(allocs) if a.height is not None)
        for v in variants
    )
    assert peak == best_variant_peak


def test_greedy_by_all_deterministic() -> None:
    allocator = GreedyByAllAllocator()
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=0, end=i % 7 + 1)
        for i in range(20)
    )
    result1 = allocator.allocate(allocs)
    result2 = allocator.allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))
