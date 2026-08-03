#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy import GreedyBySizeAllocator
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool


def test_basic_creation_with_int_id() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.id == 201
    assert len(pool.allocations) == 1
    assert pool.offset is None


def test_basic_creation_with_str_id() -> None:
    alloc = Allocation(id="alloc_101", size=100, start=0, end=10, offset=0)
    pool = Pool(id="pool_main", allocations=(alloc,))
    assert pool.id == "pool_main"
    assert len(pool.allocations) == 1
    assert pool.offset is None


def test_empty_pool() -> None:
    pool = Pool(id=1, allocations=())
    assert len(pool.allocations) == 0
    assert pool.size == 0
    assert pool.pressure == 0
    assert pool.is_allocated is True


def test_creation_with_offset() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,), offset=50)
    assert pool.offset == 50


def test_negative_offset() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    with pytest.raises(ValueError, match="offset must be non-negative"):
        Pool(id=201, allocations=(alloc,), offset=-1)


def test_zero_offset() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,), offset=0)
    assert pool.offset == 0


def test_duplicate_allocation_ids() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=101, size=50, start=5, end=15, offset=100)
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        Pool(id=201, allocations=(alloc1, alloc2))


def test_size_single_allocation() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.size == 100


def test_size_non_overlapping_allocations() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 150


def test_size_overlapping_allocations() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=50)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 150


def test_size_completely_overlapping_allocations() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=25)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 100


def test_size_with_gaps() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=200)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 250


def test_size_unallocated_items() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="unallocated pool"):
        _ = pool.size


def test_size_mixed_allocated_unallocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="unallocated pool"):
        _ = pool.size


def test_pressure_single_allocation() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.pressure == 100


def test_pressure_all_overlapping() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    alloc3 = Allocation(id=103, size=75, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 225


def test_pressure_no_overlap() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=5)
    alloc2 = Allocation(id=102, size=50, start=5, end=10)
    alloc3 = Allocation(id=103, size=75, start=10, end=15)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 100


def test_pressure_partial_overlap() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=5, end=15)
    alloc3 = Allocation(id=103, size=75, start=10, end=20)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 150


def test_pressure_empty_pool() -> None:
    pool = Pool(id=1, allocations=())
    assert pool.pressure == 0


def test_is_allocated_all_allocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is True


def test_is_allocated_none_allocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is False


def test_is_allocated_partially_allocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is False


def test_is_allocated_empty_pool() -> None:
    pool = Pool(id=1, allocations=())
    assert pool.is_allocated is True


def test_overlaps_pools_with_overlap() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=50)
    assert pool1.overlaps(pool2)
    assert pool2.overlaps(pool1)


def test_overlaps_pools_adjacent() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=100)
    assert not pool1.overlaps(pool2)
    assert not pool2.overlaps(pool1)


def test_overlaps_pools_separated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=200)
    assert not pool1.overlaps(pool2)
    assert not pool2.overlaps(pool1)


def test_overlaps_pools_exact_match() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=0)
    assert pool1.overlaps(pool2)
    assert pool2.overlaps(pool1)


def test_overlaps_pool_without_offset() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,))
    pool2 = Pool(id=202, allocations=(alloc2,), offset=0)
    assert not pool1.overlaps(pool2)
    assert not pool2.overlaps(pool1)


def test_overlaps_both_pools_without_offset() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,))
    pool2 = Pool(id=202, allocations=(alloc2,))
    assert not pool1.overlaps(pool2)


def test_overlaps_single_byte() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=99)
    assert pool1.overlaps(pool2)
    assert pool2.overlaps(pool1)


def test_with_allocations_replace() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1,), offset=50)
    new_pool = pool.with_allocations((alloc2,))
    assert len(new_pool.allocations) == 1
    assert new_pool.allocations[0].id == 102
    assert new_pool.id == pool.id
    assert new_pool.offset == pool.offset


def test_with_allocations_immutability() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1,))
    new_pool = pool.with_allocations((alloc2,))
    assert pool is not new_pool
    assert len(pool.allocations) == 1
    assert pool.allocations[0].id == 101
    assert len(new_pool.allocations) == 1
    assert new_pool.allocations[0].id == 102


def test_with_allocations_empty() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    new_pool = pool.with_allocations(())
    assert len(new_pool.allocations) == 0


def test_cannot_modify_id() -> None:
    pool = Pool(id=201, allocations=())
    with pytest.raises(AttributeError):
        pool.id = "new_id"  # type: ignore[misc]


def test_cannot_modify_allocations() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    with pytest.raises(AttributeError):
        pool.allocations = ()  # type: ignore[misc]


def test_cannot_modify_offset() -> None:
    pool = Pool(id=201, allocations=(), offset=50)
    with pytest.raises(AttributeError):
        pool.offset = 100  # type: ignore[misc]


def test_large_values() -> None:
    alloc1 = Allocation(id=101, size=10**12, start=0, end=100, offset=0)
    alloc2 = Allocation(id=102, size=10**11, start=0, end=100, offset=10**12)
    pool = Pool(id=999, allocations=(alloc1, alloc2), offset=10**15)
    assert pool.size == 10**12 + 10**11
    assert pool.pressure == 10**12 + 10**11
    assert pool.offset == 10**15


def test_multiple_allocations_complex() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=5, end=15, offset=150)
    alloc3 = Allocation(id=103, size=75, start=10, end=20, offset=50)
    pool = Pool(id=300, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 150
    assert pool.size == 200
    assert pool.is_allocated is True


def test_allocate_with_allocator() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=5, end=15)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is False

    allocator = NaiveAllocator()
    allocated_pool = pool.allocate(allocator)

    assert allocated_pool.is_allocated is True
    assert allocated_pool.id == pool.id
    assert allocated_pool.offset == pool.offset
    assert len(allocated_pool.allocations) == 2
    assert pool.is_allocated is False


def test_pool_allocate_rejects_allocator_returning_different_set() -> None:
    from omnimalloc.allocators.base import BaseAllocator

    class DroppingAllocator(BaseAllocator):
        def _allocate(
            self, allocations: tuple[Allocation, ...]
        ) -> tuple[Allocation, ...]:
            return tuple(a.with_offset(0) for a in allocations[:-1])

    pool = Pool(
        id="p",
        allocations=(
            Allocation(id=1, size=10, start=0, end=5),
            Allocation(id=2, size=10, start=0, end=5),
        ),
    )
    with pytest.raises(ValueError, match="different allocation set"):
        pool.allocate(DroppingAllocator())


def test_size_counts_gap_below_lowest_allocation() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=1000)
    pool = Pool(id="p", allocations=(alloc,))
    assert pool.size == 1100


def test_any_allocated_empty_pool() -> None:
    pool = Pool(id=1, allocations=())
    assert pool.any_allocated is False
    assert pool.is_allocated is True


def test_any_allocated_none_placed() -> None:
    pool = Pool(id=1, allocations=(Allocation(id=1, size=10, start=0, end=5),))
    assert pool.any_allocated is False


def test_any_allocated_partially_placed() -> None:
    alloc1 = Allocation(id=1, size=10, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=10, start=0, end=5)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    assert pool.any_allocated is True
    assert pool.is_allocated is False


def test_from_allocations_wraps_sequence() -> None:
    allocations = [Allocation(id=1, size=10, start=0, end=5)]
    pool = Pool.from_allocations(allocations)
    assert pool.allocations == tuple(allocations)


def test_from_allocations_rejects_non_allocation_elements() -> None:
    with pytest.raises(TypeError, match="Expected Allocation"):
        Pool.from_allocations((1, 2))


def test_from_allocations_rejects_non_sequence() -> None:
    with pytest.raises(TypeError, match="Unsupported entity type"):
        Pool.from_allocations("abc")


def test_allocate_preserves_allocation_order() -> None:
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=0, end=5) for i in range(8)
    )
    pool = Pool(id=1, allocations=allocations)
    allocated = pool.allocate(GreedyBySizeAllocator())
    assert allocated.is_allocated is True
    assert tuple(a.id for a in allocated.allocations) == tuple(range(8))
    assert tuple(a.size for a in allocated.allocations) == tuple(
        a.size for a in allocations
    )


def test_pool_coerces_a_list_of_allocations_to_a_tuple() -> None:
    pool = Pool(id="p", allocations=[Allocation(id=1, size=10, start=0, end=5)])
    assert isinstance(pool.allocations, tuple)
    assert hash(pool)


def test_pool_rejects_non_allocation_members() -> None:
    with pytest.raises(TypeError, match="Expected Allocation"):
        Pool(id="p", allocations=[1, 2])
