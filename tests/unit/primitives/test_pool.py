#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool


def test_basic_creation_with_int_id_simple() -> None:
    """Test creating a pool with integer id."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.id == 201
    assert len(pool.allocations) == 1
    assert pool.offset is None


def test_basic_creation_with_int_id() -> None:
    """Test creating a pool with integer id."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=42, allocations=(alloc,))
    assert pool.id == 42


def test_basic_creation_with_str_id() -> None:
    """Test creating a pool with string id."""
    alloc = Allocation(id="alloc_101", size=100, start=0, end=10, offset=0)
    pool = Pool(id="pool_main", allocations=(alloc,))
    assert pool.id == "pool_main"
    assert len(pool.allocations) == 1
    assert pool.offset is None


def test_empty_pool() -> None:
    """Test creating a pool with no allocations."""
    pool = Pool(id=1, allocations=())
    assert len(pool.allocations) == 0
    assert pool.size == 0
    assert pool.pressure == 0
    assert pool.is_allocated is True


def test_creation_with_offset() -> None:
    """Test creating a pool with offset."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,), offset=50)
    assert pool.offset == 50


def test_negative_offset() -> None:
    """Test that negative offset raises ValueError."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    with pytest.raises(ValueError, match="offset must be non-negative"):
        Pool(id=201, allocations=(alloc,), offset=-1)


def test_zero_offset() -> None:
    """Test that zero offset is valid."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,), offset=0)
    assert pool.offset == 0


def test_duplicate_allocation_ids() -> None:
    """Test that duplicate allocation ids raise ValueError."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=101, size=50, start=5, end=15, offset=100)
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        Pool(id=201, allocations=(alloc1, alloc2))


def test_size_single_allocation() -> None:
    """Test size calculation with single allocation."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.size == 100


def test_size_non_overlapping_allocations() -> None:
    """Test size calculation with non-overlapping allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 150


def test_size_overlapping_allocations() -> None:
    """Test size calculation with overlapping allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=50)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 150


def test_size_completely_overlapping_allocations() -> None:
    """Test size calculation with completely overlapping allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=25)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 100


def test_size_with_gaps() -> None:
    """Test size calculation with gaps between allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=200)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.size == 250


def test_size_unallocated_items() -> None:
    """Test size calculation with unallocated items (no offset)."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="unallocated pool"):
        _ = pool.size


def test_size_mixed_allocated_unallocated() -> None:
    """Test size calculation with mix of allocated and unallocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="unallocated pool"):
        _ = pool.size


def test_total_size_single_allocation() -> None:
    """Test total_size calculation with single allocation."""
    alloc = Allocation(id=101, size=100, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.total_size == 100


def test_total_size_multiple_allocations() -> None:
    """Test total_size calculation with multiple allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    alloc3 = Allocation(id=103, size=75, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.total_size == 225


def test_total_size_empty_pool() -> None:
    """Test total_size calculation with empty pool."""
    pool = Pool(id=1, allocations=())
    assert pool.total_size == 0


def test_pressure_single_allocation() -> None:
    """Test pressure calculation with single allocation."""
    alloc = Allocation(id=101, size=100, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc,))
    assert pool.pressure == 100


def test_pressure_all_overlapping() -> None:
    """Test pressure when all allocations overlap temporally."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    alloc3 = Allocation(id=103, size=75, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 225


def test_pressure_no_overlap() -> None:
    """Test pressure when allocations don't overlap temporally."""
    alloc1 = Allocation(id=101, size=100, start=0, end=5)
    alloc2 = Allocation(id=102, size=50, start=5, end=10)
    alloc3 = Allocation(id=103, size=75, start=10, end=15)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 100


def test_pressure_partial_overlap() -> None:
    """Test pressure with partial temporal overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=5, end=15)
    alloc3 = Allocation(id=103, size=75, start=10, end=20)
    pool = Pool(id=201, allocations=(alloc1, alloc2, alloc3))
    assert pool.pressure == 150


def test_pressure_empty_pool() -> None:
    """Test pressure calculation with empty pool."""
    pool = Pool(id=1, allocations=())
    assert pool.pressure == 0


def test_is_allocated_all_allocated() -> None:
    """Test is_allocated when all allocations have offsets."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is True


def test_is_allocated_none_allocated() -> None:
    """Test is_allocated when no allocations have offsets."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is False


def test_is_allocated_partially_allocated() -> None:
    """Test is_allocated when some allocations have offsets."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10)
    pool = Pool(id=201, allocations=(alloc1, alloc2))
    assert pool.is_allocated is False


def test_is_allocated_empty_pool() -> None:
    """Test is_allocated for empty pool."""
    pool = Pool(id=1, allocations=())
    assert pool.is_allocated is True


def test_overlaps_pools_with_overlap() -> None:
    """Test overlap detection with overlapping pools."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=50)
    assert pool1.overlaps(pool2)
    assert pool2.overlaps(pool1)


def test_overlaps_pools_adjacent() -> None:
    """Test overlap detection with adjacent pools."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=100)
    assert not pool1.overlaps(pool2)
    assert not pool2.overlaps(pool1)


def test_overlaps_pools_separated() -> None:
    """Test overlap detection with separated pools."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=200)
    assert not pool1.overlaps(pool2)
    assert not pool2.overlaps(pool1)


def test_overlaps_pools_exact_match() -> None:
    """Test overlap detection with exact same location."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=0)
    assert pool1.overlaps(pool2)
    assert pool2.overlaps(pool1)


def test_overlaps_pool_without_offset() -> None:
    """Test overlap returns False when pool has no offset."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,))
    pool2 = Pool(id=202, allocations=(alloc2,), offset=0)
    assert not pool1.overlaps(pool2)
    assert not pool2.overlaps(pool1)


def test_overlaps_both_pools_without_offset() -> None:
    """Test overlap returns False when both pools have no offset."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,))
    pool2 = Pool(id=202, allocations=(alloc2,))
    assert not pool1.overlaps(pool2)


def test_overlaps_single_byte() -> None:
    """Test overlap detection with single byte overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=201, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=202, allocations=(alloc2,), offset=99)
    assert pool1.overlaps(pool2)
    assert pool2.overlaps(pool1)


def test_with_allocations_replace() -> None:
    """Test replacing allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=100)
    pool = Pool(id=201, allocations=(alloc1,), offset=50)
    new_pool = pool.with_allocations((alloc2,))
    assert len(new_pool.allocations) == 1
    assert new_pool.allocations[0].id == 102
    assert new_pool.id == pool.id
    assert new_pool.offset == pool.offset


def test_with_allocations_immutability() -> None:
    """Test that original pool is not modified."""
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
    """Test with_allocations with empty tuple."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    new_pool = pool.with_allocations(())
    assert len(new_pool.allocations) == 0


def test_cannot_modify_id() -> None:
    """Test that id cannot be modified."""
    pool = Pool(id=201, allocations=())
    with pytest.raises(AttributeError):
        pool.id = "new_id"  # type: ignore[misc]


def test_cannot_modify_allocations() -> None:
    """Test that allocations cannot be modified."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=201, allocations=(alloc,))
    with pytest.raises(AttributeError):
        pool.allocations = ()  # type: ignore[misc]


def test_cannot_modify_offset() -> None:
    """Test that offset cannot be modified."""
    pool = Pool(id=201, allocations=(), offset=50)
    with pytest.raises(AttributeError):
        pool.offset = 100  # type: ignore[misc]


def test_large_values() -> None:
    """Test pool with large values."""
    alloc1 = Allocation(id=101, size=10**12, start=0, end=100, offset=0)
    alloc2 = Allocation(id=102, size=10**11, start=0, end=100, offset=10**12)
    pool = Pool(id=999, allocations=(alloc1, alloc2), offset=10**15)
    assert pool.size == 10**12 + 10**11
    assert pool.total_size == 10**12 + 10**11
    assert pool.pressure == 10**12 + 10**11
    assert pool.offset == 10**15


def test_multiple_allocations_complex() -> None:
    """Test pool with multiple complex allocations."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=5, end=15, offset=150)
    alloc3 = Allocation(id=103, size=75, start=10, end=20, offset=50)
    pool = Pool(id=300, allocations=(alloc1, alloc2, alloc3))
    assert pool.total_size == 225
    assert pool.pressure == 150
    assert pool.size == 200
    assert pool.is_allocated is True


def test_allocate_with_allocator() -> None:
    """Test allocate method with an allocator."""
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
    # Original pool should be unchanged
    assert pool.is_allocated is False
