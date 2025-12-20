#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.memory import Memory
from omnimalloc.primitives.pool import Pool


def test_basic_creation_with_int_id_simple() -> None:
    """Test creating a memory with integer id."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    assert memory.id == 301
    assert len(memory.pools) == 1
    assert memory.size is None


def test_basic_creation_with_int_id() -> None:
    """Test creating a memory with integer id."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=42, pools=(pool,))
    assert memory.id == 42


def test_basic_creation_with_str_id() -> None:
    """Test creating a memory with string id."""
    alloc = Allocation(id="alloc_101", size=100, start=0, end=10, offset=0)
    pool = Pool(id="pool_211", allocations=(alloc,))
    memory = Memory(id="mem_ddr", pools=(pool,))
    assert memory.id == "mem_ddr"
    assert len(memory.pools) == 1
    assert memory.size is None


def test_creation_with_size() -> None:
    """Test creating a memory with specified size."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=1000)
    assert memory.size == 1000


def test_creation_with_multiple_pools() -> None:
    """Test creating a memory with multiple pools."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert len(memory.pools) == 2


def test_empty_memory() -> None:
    """Test creating a memory with no pools."""
    memory = Memory(id=1, pools=())
    assert len(memory.pools) == 0
    assert memory.used_size == 0
    assert memory.is_allocated is True


def test_negative_size() -> None:
    """Test that negative size raises ValueError."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    with pytest.raises(ValueError, match="size must be non-negative"):
        Memory(id=301, pools=(pool,), size=-1)


def test_zero_size() -> None:
    """Test that zero size is valid."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=0)
    assert memory.size == 0


def test_duplicate_pool_ids() -> None:
    """Test that duplicate pool ids raise ValueError."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=211, allocations=(alloc2,))
    with pytest.raises(ValueError, match="pool ids must be unique"):
        Memory(id=301, pools=(pool1, pool2))


def test_used_size_single_pool() -> None:
    """Test used_size calculation with single pool."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    assert memory.used_size == 100


def test_used_size_multiple_pools() -> None:
    """Test used_size is sum of pool sizes."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.used_size == 300


def test_used_size_empty_memory() -> None:
    """Test used_size of empty memory is zero."""
    memory = Memory(id=1, pools=())
    assert memory.used_size == 0


def test_free_size_with_size() -> None:
    """Test free_size calculation when size is set."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=1000)
    assert memory.free_size == 900


def test_free_size_without_size() -> None:
    """Test free_size returns None when size is not set."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    assert memory.free_size is None


def test_free_size_zero() -> None:
    """Test free_size when used_size equals size."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=100)
    assert memory.free_size == 0


def test_free_size_empty_memory() -> None:
    """Test free_size equals size for empty memory."""
    memory = Memory(id=1, pools=(), size=1000)
    assert memory.free_size == 1000


def test_utilization_with_size() -> None:
    """Test utilization calculation when size is set."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=1000)
    assert memory.utilization == 0.1


def test_utilization_without_size() -> None:
    """Test utilization returns None when size is not set."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    assert memory.utilization is None


def test_utilization_full() -> None:
    """Test utilization when memory is fully used."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=100)
    assert memory.utilization == 1.0


def test_utilization_empty() -> None:
    """Test utilization when memory is empty."""
    memory = Memory(id=1, pools=(), size=1000)
    assert memory.utilization == 0.0


def test_utilization_zero_size() -> None:
    """Test utilization returns zero for zero size."""
    memory = Memory(id=1, pools=(), size=0)
    assert memory.utilization == 0.0


def test_is_allocated_all_pools_allocated() -> None:
    """Test is_allocated when all pools are allocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.is_allocated is True


def test_is_allocated_none_allocated() -> None:
    """Test is_allocated when no pools are allocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=200, start=0, end=10)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.is_allocated is False


def test_is_allocated_partially_allocated() -> None:
    """Test is_allocated when some pools are allocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.is_allocated is False


def test_is_allocated_empty_memory() -> None:
    """Test is_allocated for empty memory is True."""
    memory = Memory(id=1, pools=())
    assert memory.is_allocated is True


def test_with_pools_replace() -> None:
    """Test replacing pools."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1,), size=1000)
    new_memory = memory.with_pools((pool2,))
    assert len(new_memory.pools) == 1
    assert new_memory.pools[0].id == 212
    assert new_memory.id == memory.id
    assert new_memory.size == memory.size


def test_with_pools_immutability() -> None:
    """Test that original memory is not modified."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1,))
    new_memory = memory.with_pools((pool2,))
    assert memory is not new_memory
    assert len(memory.pools) == 1
    assert memory.pools[0].id == 211
    assert len(new_memory.pools) == 1
    assert new_memory.pools[0].id == 212


def test_with_pools_empty() -> None:
    """Test with_pools with empty tuple."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    new_memory = memory.with_pools(())
    assert len(new_memory.pools) == 0


def test_cannot_modify_id() -> None:
    """Test that id cannot be modified."""
    memory = Memory(id=301, pools=())
    with pytest.raises(AttributeError):
        memory.id = "new_id"  # type: ignore[misc]


def test_cannot_modify_pools() -> None:
    """Test that pools cannot be modified."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    with pytest.raises(AttributeError):
        memory.pools = ()  # type: ignore[misc]


def test_cannot_modify_size() -> None:
    """Test that size cannot be modified."""
    memory = Memory(id=301, pools=(), size=1000)
    with pytest.raises(AttributeError):
        memory.size = 2000  # type: ignore[misc]


def test_large_values() -> None:
    """Test memory with large values."""
    alloc1 = Allocation(id=101, size=10**12, start=0, end=100, offset=0)
    alloc2 = Allocation(id=102, size=10**11, start=0, end=100, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=999, pools=(pool1, pool2), size=10**15)
    assert memory.used_size == 10**12 + 10**11
    assert memory.free_size == 10**15 - (10**12 + 10**11)
    assert memory.utilization == (10**12 + 10**11) / 10**15


def test_complex_memory_structure() -> None:
    """Test memory with complex pool structure."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=5, end=15, offset=100)
    alloc3 = Allocation(id=103, size=75, start=10, end=20, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1, alloc2))
    pool2 = Pool(id=212, allocations=(alloc3,))
    memory = Memory(id=400, pools=(pool1, pool2), size=500)
    assert memory.used_size == pool1.size + pool2.size
    assert memory.is_allocated is True


def test_allocate_with_allocator() -> None:
    """Test allocate method with an allocator."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=5, end=15)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2), size=1000)
    assert memory.is_allocated is False

    allocator = NaiveAllocator()
    allocated_memory = memory.allocate(allocator)

    assert allocated_memory.is_allocated is True
    assert allocated_memory.id == memory.id
    assert allocated_memory.size == memory.size
    assert len(allocated_memory.pools) == 2
    # Original memory should be unchanged
    assert memory.is_allocated is False
