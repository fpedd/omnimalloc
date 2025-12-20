#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.memory import Memory
from omnimalloc.primitives.pool import Pool
from omnimalloc.primitives.system import System


def test_basic_creation_with_int_id_simple() -> None:
    """Test creating a system with integer id."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=311, pools=(pool,))
    system = System(id=401, memories=(memory,))
    assert system.id == 401
    assert len(system.memories) == 1


def test_basic_creation_with_int_id() -> None:
    """Test creating a system with integer id."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=311, pools=(pool,))
    system = System(id=42, memories=(memory,))
    assert system.id == 42


def test_basic_creation_with_str_id() -> None:
    """Test creating a system with string id."""
    alloc = Allocation(id="alloc_101", size=100, start=0, end=10, offset=0)
    pool = Pool(id="pool_211", allocations=(alloc,))
    memory = Memory(id="mem_311", pools=(pool,))
    system = System(id="sys_main", memories=(memory,))
    assert system.id == "sys_main"
    assert len(system.memories) == 1


def test_creation_with_multiple_memories() -> None:
    """Test creating a system with multiple memories."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1, memory2))
    assert len(system.memories) == 2


def test_empty_system() -> None:
    """Test creating a system with no memories."""
    system = System(id=1, memories=())
    assert len(system.memories) == 0
    assert system.is_allocated is True


def test_is_allocated_all_memories_allocated() -> None:
    """Test is_allocated when all memories are allocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1, memory2))
    assert system.is_allocated is True


def test_is_allocated_none_allocated() -> None:
    """Test is_allocated when no memories are allocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=200, start=0, end=10)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1, memory2))
    assert system.is_allocated is False


def test_is_allocated_partially_allocated() -> None:
    """Test is_allocated when some memories are allocated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1, memory2))
    assert system.is_allocated is False


def test_is_allocated_empty_system() -> None:
    """Test is_allocated for empty system is True."""
    system = System(id=1, memories=())
    assert system.is_allocated is True


def test_with_memories_replace() -> None:
    """Test replacing memories."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1,))
    new_system = system.with_memories((memory2,))
    assert len(new_system.memories) == 1
    assert new_system.memories[0].id == 312
    assert new_system.id == system.id


def test_with_memories_immutability() -> None:
    """Test that original system is not modified."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1,))
    new_system = system.with_memories((memory2,))
    assert system is not new_system
    assert len(system.memories) == 1
    assert system.memories[0].id == 311
    assert len(new_system.memories) == 1
    assert new_system.memories[0].id == 312


def test_with_memories_empty() -> None:
    """Test with_memories with empty tuple."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=311, pools=(pool,))
    system = System(id=401, memories=(memory,))
    new_system = system.with_memories(())
    assert len(new_system.memories) == 0


def test_cannot_modify_id() -> None:
    """Test that id cannot be modified."""
    system = System(id=401, memories=())
    with pytest.raises(AttributeError):
        system.id = 500  # type: ignore[misc]


def test_cannot_modify_memories() -> None:
    """Test that memories cannot be modified."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=311, pools=(pool,))
    system = System(id=401, memories=(memory,))
    with pytest.raises(AttributeError):
        system.memories = ()  # type: ignore[misc]


def test_complex_system_structure() -> None:
    """Test system with complex nested structure."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=5, end=15, offset=100)
    alloc3 = Allocation(id=103, size=75, start=10, end=20, offset=0)
    alloc4 = Allocation(id=104, size=200, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1, alloc2))
    pool2 = Pool(id=212, allocations=(alloc3,))
    pool3 = Pool(id=213, allocations=(alloc4,))
    memory1 = Memory(id=311, pools=(pool1, pool2), size=500)
    memory2 = Memory(id=312, pools=(pool3,), size=300)
    system = System(id=401, memories=(memory1, memory2))
    assert len(system.memories) == 2
    assert system.is_allocated is True


def test_hierarchical_allocation_status() -> None:
    """Test is_allocated propagates through hierarchy."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=5, end=15)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,))
    memory2 = Memory(id=312, pools=(pool2,))
    system = System(id=401, memories=(memory1, memory2))
    assert memory1.is_allocated is True
    assert memory2.is_allocated is False
    assert system.is_allocated is False


def test_single_memory_system() -> None:
    """Test system with single memory."""
    alloc = Allocation(id=101, size=100, start=0, end=10, offset=0)
    pool = Pool(id=211, allocations=(alloc,))
    memory = Memory(id=311, pools=(pool,), size=1000)
    system = System(id=401, memories=(memory,))
    assert len(system.memories) == 1
    assert system.is_allocated is True


def test_multiple_pools_per_memory() -> None:
    """Test system with multiple pools per memory."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=200, start=0, end=10, offset=0)
    alloc3 = Allocation(id=103, size=150, start=0, end=10, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    pool3 = Pool(id=213, allocations=(alloc3,))
    memory1 = Memory(id=311, pools=(pool1, pool2))
    memory2 = Memory(id=312, pools=(pool3,))
    system = System(id=401, memories=(memory1, memory2))
    assert memory1.used_size == 300
    assert memory2.used_size == 150
    assert system.is_allocated is True


def test_large_system() -> None:
    """Test system with large values."""
    alloc1 = Allocation(id=101, size=10**12, start=0, end=100, offset=0)
    alloc2 = Allocation(id=102, size=10**11, start=0, end=100, offset=0)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2,))
    memory1 = Memory(id=311, pools=(pool1,), size=10**15)
    memory2 = Memory(id=312, pools=(pool2,), size=10**14)
    system = System(id=999, memories=(memory1, memory2))
    assert memory1.used_size == 10**12
    assert memory2.used_size == 10**11
    assert system.is_allocated is True


def test_allocate_with_allocator() -> None:
    """Test allocate method with an allocator."""

    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=50, start=5, end=15)
    alloc3 = Allocation(id=103, size=75, start=10, end=20)
    pool1 = Pool(id=211, allocations=(alloc1,))
    pool2 = Pool(id=212, allocations=(alloc2, alloc3))
    memory1 = Memory(id=311, pools=(pool1,), size=1000)
    memory2 = Memory(id=312, pools=(pool2,), size=500)
    system = System(id=401, memories=(memory1, memory2))
    assert system.is_allocated is False

    allocator = NaiveAllocator()
    allocated_system = system.allocate(allocator)

    assert allocated_system.is_allocated is True
    assert allocated_system.id == system.id
    assert len(allocated_system.memories) == 2
    # Original system should be unchanged
    assert system.is_allocated is False
