#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.memory import Memory
from omnimalloc.primitives.pool import Pool
from omnimalloc.primitives.system import System
from omnimalloc.validate import validate_allocation


def test_validate_pool_valid_single_allocation() -> None:
    """Test validation of a pool with a single valid allocation."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,))
    assert validate_allocation(pool, raise_on_error=False) is True


def test_validate_pool_valid_multiple_non_overlapping() -> None:
    """Test validation of a pool with multiple non-overlapping allocations."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    assert validate_allocation(pool, raise_on_error=False) is True


def test_validate_pool_overlapping_temporal() -> None:
    """Test validation fails for allocations with temporal overlap."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    assert validate_allocation(pool, raise_on_error=False) is False


def test_validate_pool_overlapping_temporal_raises() -> None:
    """Test validation raises for allocations with temporal overlap."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match=r"Validation .* failed"):
        validate_allocation(pool, raise_on_error=True)


def test_validate_pool_duplicate_allocation_ids() -> None:
    """Test validation fails for duplicate allocation IDs (caught at Pool creation)."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=1, size=100, start=5, end=10, offset=100)
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        Pool(id=1, allocations=(alloc1, alloc2))


def test_validate_pool_unallocated_with_require_allocated_true() -> None:
    """Test validation fails for unallocated allocations when require_allocated=True."""
    alloc = Allocation(id=1, size=100, start=0, end=10)  # No offset
    pool = Pool(id=1, allocations=(alloc,))
    assert (
        validate_allocation(pool, raise_on_error=False, require_allocated=True) is False
    )


def test_validate_pool_unallocated_with_require_allocated_false() -> None:
    """Test validation for unallocated allocations with require_allocated=False."""
    alloc = Allocation(id=1, size=100, start=0, end=10)  # No offset
    pool = Pool(id=1, allocations=(alloc,))
    assert (
        validate_allocation(pool, raise_on_error=False, require_allocated=False) is True
    )


def test_validate_pool_empty() -> None:
    """Test validation of empty pool."""
    pool = Pool(id=1, allocations=())
    assert validate_allocation(pool, raise_on_error=False) is True


def test_validate_memory_valid_single_pool() -> None:
    """Test validation of memory with a single valid pool."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)
    memory = Memory(id=1, pools=(pool,))
    assert validate_allocation(memory, raise_on_error=False) is True


def test_validate_memory_valid_multiple_non_overlapping_pools() -> None:
    """Test validation of memory with multiple non-overlapping pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    memory = Memory(id=1, pools=(pool1, pool2))
    assert validate_allocation(memory, raise_on_error=False) is True


def test_validate_memory_overlapping_pools() -> None:
    """Test validation fails for spatially overlapping pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=50)  # Overlaps with pool1
    memory = Memory(id=1, pools=(pool1, pool2))
    assert validate_allocation(memory, raise_on_error=False) is False


def test_validate_memory_overlapping_pools_raises() -> None:
    """Test validation raises for spatially overlapping pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=50)
    memory = Memory(id=1, pools=(pool1, pool2))
    with pytest.raises(ValueError, match=r"Validation .* failed"):
        validate_allocation(memory, raise_on_error=True)


def test_validate_memory_duplicate_pool_ids() -> None:
    """Test validation fails for duplicate pool IDs (caught at Memory creation)."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=1, allocations=(alloc2,), offset=200)
    with pytest.raises(ValueError, match="pool ids must be unique"):
        Memory(id=1, pools=(pool1, pool2))


def test_validate_memory_invalid_allocations_in_pool() -> None:
    """Test validation fails when a pool contains overlapping allocations."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id=1, pools=(pool,))
    assert validate_allocation(memory, raise_on_error=False) is False


def test_validate_memory_empty() -> None:
    """Test validation of empty memory."""
    memory = Memory(id=1, pools=())
    assert validate_allocation(memory, raise_on_error=False) is True


def test_validate_system_valid_single_memory() -> None:
    """Test validation of system with a single valid memory."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))
    assert validate_allocation(system, raise_on_error=False) is True


def test_validate_system_valid_multiple_memories() -> None:
    """Test validation of system with multiple valid memories."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=0)
    memory1 = Memory(id=1, pools=(pool1,))
    memory2 = Memory(id=2, pools=(pool2,))
    system = System(id=1, memories=(memory1, memory2))
    assert validate_allocation(system, raise_on_error=False) is True


def test_validate_system_duplicate_memory_ids() -> None:
    """Test validation fails for duplicate memory IDs (caught at System creation)."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=0)
    memory1 = Memory(id=1, pools=(pool1,))
    memory2 = Memory(id=1, pools=(pool2,))
    with pytest.raises(ValueError, match="memory ids must be unique"):
        System(id=1, memories=(memory1, memory2))


def test_validate_system_invalid_memory() -> None:
    """Test validation fails when a memory contains invalid pools."""
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))
    assert validate_allocation(system, raise_on_error=False) is False


def test_validate_system_empty() -> None:
    """Test validation of empty system."""
    system = System(id=1, memories=())
    assert validate_allocation(system, raise_on_error=False) is True


def test_validate_unsupported_type() -> None:
    """Test validation raises TypeError for unsupported entity type."""
    with pytest.raises(TypeError, match="Unsupported entity type"):
        validate_allocation("invalid_entity", raise_on_error=False)  # type: ignore[arg-type]


def test_validate_allocation_directly() -> None:
    """Test that validate_allocation() does not accept Allocation directly."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    with pytest.raises(TypeError, match="Unsupported entity type"):
        validate_allocation(alloc, raise_on_error=False)  # type: ignore[arg-type]


def test_validate_mixed_allocated_unallocated_require_false() -> None:
    """Test validation with mixed allocation states, require_allocated=False."""
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=10, end=15)  # Unallocated
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    assert (
        validate_allocation(pool, raise_on_error=False, require_allocated=False) is True
    )


def test_validate_complex_hierarchy() -> None:
    """Test validation of a complex System hierarchy."""
    # Create two valid memories with multiple pools each
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=10, offset=0)
    alloc3 = Allocation(id=3, size=50, start=0, end=10, offset=0)
    alloc4 = Allocation(id=4, size=75, start=10, end=18, offset=0)  # Non-overlapping

    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    pool3 = Pool(id=3, allocations=(alloc3, alloc4), offset=0)

    memory1 = Memory(id=1, pools=(pool1, pool2))
    memory2 = Memory(id=2, pools=(pool3,))

    system = System(id=1, memories=(memory1, memory2))
    assert validate_allocation(system, raise_on_error=False) is True


def test_validate_complex_hierarchy_with_error() -> None:
    """Test validation of a complex System hierarchy with error deep in hierarchy."""
    # Create a system with an error in one of the pools
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=3, end=10, offset=0)  # Overlaps alloc1
    alloc3 = Allocation(id=3, size=50, start=0, end=10, offset=0)

    pool1 = Pool(id=1, allocations=(alloc1, alloc2), offset=0)  # Invalid
    pool2 = Pool(id=2, allocations=(alloc3,), offset=0)

    memory1 = Memory(id=1, pools=(pool1,))
    memory2 = Memory(id=2, pools=(pool2,))

    system = System(id=1, memories=(memory1, memory2))
    assert validate_allocation(system, raise_on_error=False) is False
