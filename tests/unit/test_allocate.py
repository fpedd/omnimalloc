#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocate import run_allocation
from omnimalloc.allocators.greedy import GreedyAllocator
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation, BufferKind, Memory, Pool, System


def test_allocate_pool_with_naive_allocator() -> None:
    """Test allocating a pool with naive allocator."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=150, start=5, end=10)
    pool = Pool(id=1, allocations=(alloc1, alloc2))

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    # Check that allocations were placed
    assert allocated_pool.is_allocated
    assert all(a.offset is not None for a in allocated_pool.allocations)

    # Naive allocator places sequentially
    assert allocated_pool.allocations[0].offset == 0
    assert allocated_pool.allocations[1].offset == 100


def test_allocate_pool_with_greedy_allocator() -> None:
    """Test allocating a pool with greedy allocator."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=150, start=10, end=20)
    alloc3 = Allocation(id=3, size=75, start=0, end=5)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3))

    allocator = GreedyAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert allocated_pool.is_allocated
    assert all(a.offset is not None for a in allocated_pool.allocations)


def test_allocate_memory_with_multiple_pools() -> None:
    """Test allocating memory with multiple pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=150, start=0, end=10)
    pool1 = Pool(id=1, allocations=(alloc1,))
    pool2 = Pool(id=2, allocations=(alloc2,))
    memory = Memory(id=1, pools=(pool1, pool2))

    allocator = NaiveAllocator()
    allocated_memory = run_allocation(memory, allocator)

    assert allocated_memory.is_allocated
    assert all(p.is_allocated for p in allocated_memory.pools)


def test_allocate_system_with_multiple_memories() -> None:
    """Test allocating system with multiple memories."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=150, start=0, end=5)
    pool1 = Pool(id=1, allocations=(alloc1,))
    pool2 = Pool(id=2, allocations=(alloc2,))
    memory1 = Memory(id=1, pools=(pool1,))
    memory2 = Memory(id=2, pools=(pool2,))
    system = System(id=1, memories=(memory1, memory2))

    allocator = NaiveAllocator()
    allocated_system = run_allocation(system, allocator)

    assert allocated_system.is_allocated
    assert all(m.is_allocated for m in allocated_system.memories)


def test_allocate_with_validation_success() -> None:
    """Test allocate with validation when allocation is valid."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=150, start=5, end=10)
    pool = Pool(id=1, allocations=(alloc1, alloc2))

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator, validate=True)

    assert allocated_pool.is_allocated


def test_allocate_preserves_pool_offset() -> None:
    """Test that allocate preserves pool offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    pool = Pool(id=1, allocations=(alloc,), offset=50)

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert allocated_pool.offset == 50


def test_allocate_with_buffer_kinds() -> None:
    """Test allocating with different buffer kinds."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, kind=BufferKind.WORKSPACE)
    alloc2 = Allocation(id=2, size=150, start=5, end=10, kind=BufferKind.CONSTANT)
    alloc3 = Allocation(id=3, size=75, start=10, end=15, kind=BufferKind.INPUT)
    pool = Pool(id=1, allocations=(alloc1, alloc2, alloc3))

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    # Check kinds are preserved
    assert allocated_pool.allocations[0].kind == BufferKind.WORKSPACE
    assert allocated_pool.allocations[1].kind == BufferKind.CONSTANT
    assert allocated_pool.allocations[2].kind == BufferKind.INPUT


def test_allocate_with_string_ids() -> None:
    """Test allocating with string IDs."""
    alloc1 = Allocation(id="buf_a", size=100, start=0, end=5)
    alloc2 = Allocation(id="buf_b", size=150, start=5, end=10)
    pool = Pool(id="main_pool", allocations=(alloc1, alloc2))

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert allocated_pool.is_allocated
    assert allocated_pool.id == "main_pool"
    assert allocated_pool.allocations[0].id == "buf_a"
    assert allocated_pool.allocations[1].id == "buf_b"


def test_allocate_complex_hierarchy() -> None:
    """Test allocating a complex system hierarchy."""
    # Create multiple allocations with different temporal patterns
    allocations_pool1 = [
        Allocation(id=i, size=50 + i * 10, start=i * 2, end=(i + 1) * 2)
        for i in range(5)
    ]
    allocations_pool2 = [
        Allocation(id=i + 5, size=100, start=i * 3, end=(i + 1) * 3) for i in range(3)
    ]

    pool1 = Pool(id=1, allocations=tuple(allocations_pool1))
    pool2 = Pool(id=2, allocations=tuple(allocations_pool2))

    memory1 = Memory(id=1, pools=(pool1, pool2), size=2048)

    # Second memory
    allocations_pool3 = [
        Allocation(id=i + 10, size=75, start=i * 5, end=(i + 1) * 5) for i in range(4)
    ]
    pool3 = Pool(id=3, allocations=tuple(allocations_pool3))
    memory2 = Memory(id=2, pools=(pool3,), size=1024)

    system = System(id=1, memories=(memory1, memory2))

    allocator = GreedyAllocator()
    allocated_system = run_allocation(system, allocator)

    # Verify everything is allocated
    assert allocated_system.is_allocated
    for memory in allocated_system.memories:
        assert memory.is_allocated
        for pool in memory.pools:
            assert pool.is_allocated
            for alloc in pool.allocations:
                assert alloc.is_allocated


def test_allocate_empty_pool() -> None:
    """Test allocating an empty pool."""
    pool = Pool(id=1, allocations=())

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert allocated_pool.is_allocated
    assert len(allocated_pool.allocations) == 0


def test_allocate_single_allocation() -> None:
    """Test allocating pool with single allocation."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    pool = Pool(id=1, allocations=(alloc,))

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    assert allocated_pool.is_allocated
    assert allocated_pool.allocations[0].offset == 0
    assert allocated_pool.size == 100


def test_allocate_preserves_original_properties() -> None:
    """Test that allocate preserves all original allocation properties."""
    alloc = Allocation(id="test", size=100, start=5, end=15, kind=BufferKind.OUTPUT)
    pool = Pool(id="pool", allocations=(alloc,), offset=50)

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    allocated_alloc = allocated_pool.allocations[0]
    assert allocated_alloc.id == "test"
    assert allocated_alloc.size == 100
    assert allocated_alloc.start == 5
    assert allocated_alloc.end == 15
    assert allocated_alloc.kind == BufferKind.OUTPUT
    assert allocated_alloc.offset is not None  # This is the new property


def test_allocate_returns_same_type() -> None:
    """Test that allocate returns the same type as input."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    pool = Pool(id=1, allocations=(alloc,))
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))

    allocator = NaiveAllocator()

    # Test with Pool
    result_pool = run_allocation(pool, allocator)
    assert isinstance(result_pool, Pool)

    # Test with Memory
    result_memory = run_allocation(memory, allocator)
    assert isinstance(result_memory, Memory)

    # Test with System
    result_system = run_allocation(system, allocator)
    assert isinstance(result_system, System)


def test_allocate_pool_calculates_correct_size() -> None:
    """Test that allocated pool calculates size correctly."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5)
    alloc2 = Allocation(id=2, size=150, start=5, end=10)
    pool = Pool(id=1, allocations=(alloc1, alloc2))

    allocator = NaiveAllocator()
    allocated_pool = run_allocation(pool, allocator)

    # Naive places sequentially: 0-100, 100-250
    assert allocated_pool.size == 250
    assert allocated_pool.total_size == 250  # Sum of sizes


def test_allocate_memory_calculates_used_size() -> None:
    """Test that allocated memory calculates used size correctly."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=150, start=0, end=10)
    pool1 = Pool(id=1, allocations=(alloc1,))
    pool2 = Pool(id=2, allocations=(alloc2,))
    memory = Memory(id=1, pools=(pool1, pool2), size=1000)

    allocator = NaiveAllocator()
    allocated_memory = run_allocation(memory, allocator)

    # Each pool gets its allocations placed
    assert allocated_memory.used_size == 250  # 100 + 150
    assert allocated_memory.free_size == 750  # 1000 - 250
