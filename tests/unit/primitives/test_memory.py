#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.memory import Memory
from omnimalloc.primitives.pool import Pool


def test_basic_creation_with_int_id() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    assert memory.id == 301
    assert len(memory.pools) == 1
    assert memory.size is None


def test_basic_creation_with_str_id() -> None:
    alloc = Allocation(id="alloc_101", size=100, start=0, end=10, offset=0)
    pool = Pool(id="pool_211", allocations=(alloc,))
    memory = Memory(id="mem_ddr", pools=(pool,))
    assert memory.id == "mem_ddr"
    assert len(memory.pools) == 1
    assert memory.size is None


def test_creation_with_size() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=1000)
    assert memory.size == 1000


def test_creation_with_multiple_pools() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert len(memory.pools) == 2


def test_empty_memory() -> None:
    memory = Memory(id=1, pools=())
    assert len(memory.pools) == 0
    assert memory.used_size == 0
    assert memory.is_allocated is True


def test_negative_size() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    with pytest.raises(ValueError, match="size must be non-negative"):
        Memory(id=301, pools=(pool,), size=-1)


def test_zero_size() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=0)
    assert memory.size == 0


def test_duplicate_pool_ids() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=211, allocations=(alloc2,))
    with pytest.raises(ValueError, match="pool ids must be unique"):
        Memory(id=301, pools=(pool1, pool2))


def test_used_size_single_pool() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    assert memory.used_size == 100


def test_used_size_multiple_pools() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.used_size == 300


def test_used_size_empty_memory() -> None:
    memory = Memory(id=1, pools=())
    assert memory.used_size == 0


def test_used_size_can_exceed_the_declared_size_limit() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,), size=10)
    assert memory.used_size == 100


def test_is_allocated_all_pools_allocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.is_allocated is True


def test_is_allocated_none_allocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=200, start=0, end=10)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.is_allocated is False


def test_is_allocated_partially_allocated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=301, pools=(pool1, pool2))
    assert memory.is_allocated is False


def test_is_allocated_empty_memory() -> None:
    memory = Memory(id=1, pools=())
    assert memory.is_allocated is True


def test_with_pools_replace() -> None:
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
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    new_memory = memory.with_pools(())
    assert len(new_memory.pools) == 0


def test_cannot_modify_id() -> None:
    memory = Memory(id=301, pools=())
    with pytest.raises(AttributeError):
        memory.id = "new_id"  # type: ignore[misc]


def test_cannot_modify_pools() -> None:
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=301, pools=(pool,))
    with pytest.raises(AttributeError):
        memory.pools = ()  # type: ignore[misc]


def test_cannot_modify_size() -> None:
    memory = Memory(id=301, pools=(), size=1000)
    with pytest.raises(AttributeError):
        memory.size = 2000  # type: ignore[misc]


def test_large_values() -> None:
    alloc1 = Allocation(id=101, size=10**12, start=0, end=100, offset=0)
    alloc2 = Allocation(id=102, size=10**11, start=0, end=100, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory = Memory(id=999, pools=(pool1, pool2), size=10**15)
    assert memory.used_size == 10**12 + 10**11
    assert memory.size == 10**15


def test_any_allocated_across_pools() -> None:
    placed = Pool(
        id=1, allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),)
    )
    unplaced = Pool(id=2, allocations=(Allocation(id=2, size=10, start=0, end=5),))
    memory = Memory(id=1, pools=(placed, unplaced))
    assert memory.any_allocated is True
    assert memory.is_allocated is False


def test_any_allocated_empty_memory() -> None:
    memory = Memory(id=1, pools=())
    assert memory.any_allocated is False
    assert memory.is_allocated is True


def test_complex_memory_structure() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=5, end=15, offset=100)
    alloc3 = Allocation(id=103, size=75, start=10, end=20, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1, alloc2))
    pool2 = Pool(id=212, allocations=(alloc3,))
    memory = Memory(id=400, pools=(pool1, pool2), size=500)
    assert memory.used_size == pool1.size + pool2.size
    assert memory.is_allocated is True


def test_allocate_with_allocator() -> None:
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
    assert memory.is_allocated is False


def test_allocate_stacks_unplaced_pools() -> None:
    pool1 = Pool(id="p1", allocations=(Allocation(id=1, size=100, start=0, end=5),))
    pool2 = Pool(id="p2", allocations=(Allocation(id=2, size=40, start=0, end=5),))
    memory = Memory(id="m", pools=(pool1, pool2)).allocate(NaiveAllocator())
    assert [pool.offset for pool in memory.pools] == [0, 100]
    assert memory.extent == 140


def test_allocate_preserves_pinned_pool_base() -> None:
    pool1 = Pool(
        id="p1", allocations=(Allocation(id=1, size=100, start=0, end=5),), offset=500
    )
    pool2 = Pool(id="p2", allocations=(Allocation(id=2, size=40, start=0, end=5),))
    memory = Memory(id="m", pools=(pool1, pool2)).allocate(NaiveAllocator())
    assert [pool.offset for pool in memory.pools] == [500, 0]


def test_allocate_fills_gap_below_a_pinned_pool() -> None:
    pinned = Pool(
        id="pinned", allocations=(Allocation(id=1, size=10, start=0, end=5),), offset=50
    )
    small = Pool(id="small", allocations=(Allocation(id=2, size=20, start=0, end=5),))
    large = Pool(id="large", allocations=(Allocation(id=3, size=90, start=0, end=5),))
    memory = Memory(id="m", pools=(pinned, small, large)).allocate(NaiveAllocator())
    assert [pool.offset for pool in memory.pools] == [50, 0, 60]


def test_extent_requires_placed_pools() -> None:
    pool = Pool(
        id="p", allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),)
    )
    with pytest.raises(ValueError, match="unplaced"):
        _ = Memory(id="m", pools=(pool,)).extent


def test_allocate_empty_pool_takes_a_base() -> None:
    empty = Pool(id="empty", allocations=())
    used = Pool(id="used", allocations=(Allocation(id=1, size=10, start=0, end=5),))
    memory = Memory(id="m", pools=(empty, used)).allocate(NaiveAllocator())
    assert [pool.offset for pool in memory.pools] == [0, 0]
    assert memory.extent == 10


def test_memory_coerces_a_list_of_pools_to_a_tuple() -> None:
    memory = Memory(id="m", pools=[Pool(id="p", allocations=())])
    assert isinstance(memory.pools, tuple)
    assert hash(memory)


def test_memory_rejects_non_pool_members() -> None:
    with pytest.raises(TypeError, match="Expected Pool"):
        Memory(id="m", pools=[1])


def test_allocate_rejects_overlapping_pinned_pool_bases() -> None:
    pool1 = Pool(
        id="p1", allocations=(Allocation(id=1, size=100, start=0, end=5),), offset=0
    )
    pool2 = Pool(
        id="p2", allocations=(Allocation(id=2, size=40, start=0, end=5),), offset=50
    )
    with pytest.raises(ValueError, match="pinned pools 'p1' and 'p2' already overlap"):
        Memory(id="m", pools=(pool1, pool2)).allocate(NaiveAllocator())


def test_allocate_accepts_touching_pinned_pool_bases() -> None:
    pool1 = Pool(
        id="p1", allocations=(Allocation(id=1, size=100, start=0, end=5),), offset=0
    )
    pool2 = Pool(
        id="p2", allocations=(Allocation(id=2, size=40, start=0, end=5),), offset=100
    )
    memory = Memory(id="m", pools=(pool1, pool2)).allocate(NaiveAllocator())
    assert [pool.offset for pool in memory.pools] == [0, 100]
