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
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,))
    assert validate_allocation(pool) is None


def test_validate_pool_valid_multiple_non_overlapping() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    validate_allocation(pool)


def test_validate_pool_overlapping_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match=r"Validation .* failed.*overlaps"):
        validate_allocation(pool)


def test_validate_pool_unallocated_raises() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)  # No offset
    pool = Pool(id=1, allocations=(alloc,))
    with pytest.raises(ValueError, match="is not allocated"):
        validate_allocation(pool)


def test_validate_pool_partially_allocated_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=10, end=15)  # Unallocated
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="is not allocated"):
        validate_allocation(pool)


def test_validate_pool_empty() -> None:
    pool = Pool(id=1, allocations=())
    validate_allocation(pool)


def test_validate_memory_valid_single_pool() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)
    memory = Memory(id=1, pools=(pool,))
    validate_allocation(memory)


def test_validate_memory_valid_multiple_non_overlapping_pools() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=200)
    memory = Memory(id=1, pools=(pool1, pool2))
    validate_allocation(memory)


def test_validate_memory_overlapping_pools_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=50)  # Overlaps with pool1
    memory = Memory(id=1, pools=(pool1, pool2))
    with pytest.raises(ValueError, match=r"Validation .* failed"):
        validate_allocation(memory)


def test_validate_memory_invalid_allocations_in_pool_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id=1, pools=(pool,))
    with pytest.raises(ValueError, match=r"in pool 1"):
        validate_allocation(memory)


def test_validate_memory_empty() -> None:
    memory = Memory(id=1, pools=())
    validate_allocation(memory)


def test_validate_system_valid_single_memory() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    pool = Pool(id=1, allocations=(alloc,), offset=0)
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))
    validate_allocation(system)


def test_validate_system_valid_multiple_memories() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=0, end=10, offset=0)
    pool1 = Pool(id=1, allocations=(alloc1,), offset=0)
    pool2 = Pool(id=2, allocations=(alloc2,), offset=0)
    memory1 = Memory(id=1, pools=(pool1,))
    memory2 = Memory(id=2, pools=(pool2,))
    system = System(id=1, memories=(memory1, memory2))
    validate_allocation(system)


def test_validate_system_invalid_memory_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=5, end=15, offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2), offset=0)
    memory = Memory(id=1, pools=(pool,))
    system = System(id=1, memories=(memory,))
    with pytest.raises(ValueError, match=r"in memory 1"):
        validate_allocation(system)


def test_validate_system_empty() -> None:
    system = System(id=1, memories=())
    validate_allocation(system)


def test_validate_unsupported_type() -> None:
    with pytest.raises(TypeError, match="Unsupported entity type"):
        validate_allocation("invalid_entity")  # type: ignore[arg-type]


def test_validate_allocation_directly() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    with pytest.raises(TypeError, match="Unsupported entity type"):
        validate_allocation(alloc)  # type: ignore[arg-type]


def test_validate_complex_hierarchy() -> None:
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
    validate_allocation(system)


def test_validate_complex_hierarchy_with_error_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=5, offset=0)
    alloc2 = Allocation(id=2, size=100, start=3, end=10, offset=0)  # Overlaps alloc1
    alloc3 = Allocation(id=3, size=50, start=0, end=10, offset=0)

    pool1 = Pool(id=1, allocations=(alloc1, alloc2), offset=0)  # Invalid
    pool2 = Pool(id=2, allocations=(alloc3,), offset=0)

    memory1 = Memory(id=1, pools=(pool1,))
    memory2 = Memory(id=2, pools=(pool2,))

    system = System(id=1, memories=(memory1, memory2))
    with pytest.raises(ValueError, match=r"in memory 1.*in pool 1"):
        validate_allocation(system)


def test_validate_memory_over_size_fails() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=100, start=0, end=5, offset=0),),
        offset=0,
    )
    memory = Memory(id="m", size=50, pools=(pool,))
    with pytest.raises(ValueError, match="exceeds memory size"):
        validate_allocation(memory)


def test_validate_memory_within_size_passes() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=100, start=0, end=5, offset=0),),
        offset=0,
    )
    memory = Memory(id="m", size=100, pools=(pool,))
    validate_allocation(memory)


def test_validate_pool_vector_conflict_at_same_offset_raises() -> None:
    alloc1 = Allocation(id=1, size=100, start=(0, 5), end=(1, 6), offset=0)
    alloc2 = Allocation(id=2, size=100, start=(2, 0), end=(3, 1), offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="overlaps"):
        validate_allocation(pool)


def test_validate_pool_vector_ordered_at_same_offset() -> None:
    alloc1 = Allocation(id=1, size=100, start=(0, 0), end=(2, 1), offset=0)
    alloc2 = Allocation(id=2, size=100, start=(2, 1), end=(3, 2), offset=0)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    validate_allocation(pool)


def test_validate_pool_mixed_dimensions_rejected() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=2, size=100, start=(20, 0), end=(30, 1), offset=200)
    pool = Pool(id=1, allocations=(alloc1, alloc2))
    with pytest.raises(ValueError, match="share one clock dimension"):
        validate_allocation(pool)


def test_validate_raw_allocations_valid() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=5, offset=0),
        Allocation(id=2, size=100, start=5, end=10, offset=0),
    )
    validate_allocation(allocations)


def test_validate_raw_allocations_list() -> None:
    validate_allocation([Allocation(id=1, size=10, start=0, end=5, offset=0)])


def test_validate_raw_allocations_empty() -> None:
    validate_allocation(())


def test_validate_raw_allocations_overlapping_raises() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=10, offset=0),
        Allocation(id=2, size=100, start=5, end=15, offset=50),
    )
    with pytest.raises(ValueError, match=r"Validation of 2 allocations failed"):
        validate_allocation(allocations)


def test_validate_raw_allocations_unallocated_raises() -> None:
    with pytest.raises(ValueError, match="is not allocated"):
        validate_allocation((Allocation(id=1, size=10, start=0, end=5),))


def test_validate_raw_allocations_duplicate_ids_raise() -> None:
    allocations = (
        Allocation(id=1, size=10, start=0, end=5, offset=0),
        Allocation(id=1, size=10, start=5, end=10, offset=0),
    )
    with pytest.raises(ValueError, match="duplicate id"):
        validate_allocation(allocations)


def test_validate_raw_allocations_rejects_non_allocation_elements() -> None:
    with pytest.raises(TypeError, match="Expected Allocation"):
        validate_allocation([1, 2, 3])


def test_validate_raw_allocations_complex_mix() -> None:
    valid = tuple(
        Allocation(id=i, size=50, start=5 * i, end=5 * (i + 1), offset=0)
        for i in range(10)
    )
    validate_allocation(valid)
    overlapping = (*valid, Allocation(id=99, size=50, start=12, end=18, offset=25))
    with pytest.raises(ValueError, match=r"Validation of 11 allocations failed"):
        validate_allocation(overlapping)


def test_validate_memory_unplaced_pool_raises() -> None:
    pool = Pool(
        id="p", allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),)
    )
    memory = Memory(id="m", pools=(pool,), size=100)
    with pytest.raises(ValueError, match="pool 'p' is not placed"):
        validate_allocation(memory)


def test_validate_memory_default_pools_at_same_base_raise() -> None:
    pool1 = Pool(
        id="p1", allocations=(Allocation(id="a", size=10, start=0, end=5, offset=0),)
    )
    pool2 = Pool(
        id="p2", allocations=(Allocation(id="b", size=10, start=0, end=5, offset=0),)
    )
    memory = Memory(id="m", pools=(pool1, pool2), size=100)
    with pytest.raises(ValueError, match="is not placed"):
        validate_allocation(memory)


def test_validate_memory_duplicate_ids_across_pools_raise() -> None:
    alloc = Allocation(id="shared", size=10, start=0, end=5, offset=0)
    pool1 = Pool(id="p1", allocations=(alloc,), offset=0)
    pool2 = Pool(id="p2", allocations=(alloc,), offset=100)
    memory = Memory(id="m", pools=(pool1, pool2), size=1000)
    with pytest.raises(ValueError, match="duplicate allocation id 'shared'"):
        validate_allocation(memory)


def test_validate_memory_same_id_in_separate_memories_passes() -> None:
    alloc = Allocation(id="shared", size=10, start=0, end=5, offset=0)
    memory1 = Memory(id="m1", pools=(Pool(id="p", allocations=(alloc,), offset=0),))
    memory2 = Memory(id="m2", pools=(Pool(id="p", allocations=(alloc,), offset=0),))
    validate_allocation(System(id="s", memories=(memory1, memory2)))


def test_validate_require_capacity_rejects_missing_size() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    memory = Memory(id="m", pools=(pool,))
    validate_allocation(memory)
    with pytest.raises(ValueError, match="no size declared"):
        validate_allocation(memory, require_capacity=True)


def test_validate_require_capacity_accepts_declared_size() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    memory = Memory(id="m", pools=(pool,), size=10)
    validate_allocation(memory, require_capacity=True)


def test_validate_memory_capacity_counts_pool_base() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=1000,
    )
    memory = Memory(id="m", pools=(pool,), size=100)
    with pytest.raises(ValueError, match="exceeds memory size"):
        validate_allocation(memory)


def test_validate_accepts_an_aligned_placement() -> None:
    allocations = (
        Allocation(id=1, size=64, start=0, end=5, offset=0),
        Allocation(id=2, size=64, start=0, end=5, offset=64),
    )
    validate_allocation(allocations, alignment=64)


def test_validate_rejects_a_misaligned_offset() -> None:
    allocations = (Allocation(id=1, size=10, start=0, end=5, offset=12),)
    validate_allocation(allocations)
    with pytest.raises(ValueError, match="not 64-byte aligned"):
        validate_allocation(allocations, alignment=64)


def test_validate_rejects_a_non_positive_alignment() -> None:
    allocations = (Allocation(id=1, size=10, start=0, end=5, offset=0),)
    with pytest.raises(ValueError, match="Alignment must be positive"):
        validate_allocation(allocations, alignment=0)


def test_validate_checks_alignment_inside_a_system() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=8),),
        offset=0,
    )
    system = System(id="s", memories=(Memory(id="m", pools=(pool,)),))
    with pytest.raises(ValueError, match="not 16-byte aligned"):
        validate_allocation(system, alignment=16)


def test_validate_alignment_counts_the_pool_base() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=3,
    )
    with pytest.raises(ValueError, match="address 3 is not 8-byte aligned"):
        validate_allocation(Memory(id="m", pools=(pool,)), alignment=8)


def test_validate_alignment_accepts_an_aligned_pool_base() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=8,
    )
    validate_allocation(Memory(id="m", pools=(pool,)), alignment=8)


def test_validate_alignment_counts_the_base_of_a_standalone_pool() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=4),),
        offset=4,
    )
    validate_allocation(pool, alignment=8)


def test_validate_accepts_a_memory_that_declares_no_size() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    validate_allocation(Memory(id="m", pools=(pool,)))


def test_validate_require_capacity_accepts_a_large_declared_size() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    memory = Memory(id="m", pools=(pool,), size=1 << 50)
    validate_allocation(memory, require_capacity=True)


def test_validate_partial_placement_passes_without_require_allocated() -> None:
    placed = Allocation(id=1, size=100, start=0, end=10, offset=0)
    unplaced = Allocation(id=2, size=100, start=0, end=10)
    validate_allocation((placed, unplaced), require_allocated=False)


def test_validate_partial_placement_still_catches_pin_collisions() -> None:
    first = Allocation(id=1, size=100, start=0, end=10, offset=0)
    second = Allocation(id=2, size=100, start=5, end=15, offset=50)
    unplaced = Allocation(id=3, size=100, start=0, end=10)
    with pytest.raises(ValueError, match="overlaps"):
        validate_allocation((first, second, unplaced), require_allocated=False)


def test_validate_unplaced_pool_base_passes_without_require_allocated() -> None:
    placed = Pool(
        id="p1",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    unplaced = Pool(id="p2", allocations=(Allocation(id=2, size=10, start=0, end=5),))
    validate_allocation(
        Memory(id="m", pools=(placed, unplaced)), require_allocated=False
    )


def test_validate_pinned_pool_bases_collide_without_require_allocated() -> None:
    pool1 = Pool(
        id="p1",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=0),),
        offset=0,
    )
    pool2 = Pool(
        id="p2",
        allocations=(Allocation(id=2, size=10, start=0, end=5, offset=0),),
        offset=5,
    )
    with pytest.raises(ValueError, match="overlaps with pool"):
        validate_allocation(
            Memory(id="m", pools=(pool1, pool2)), require_allocated=False
        )


def test_validate_default_still_requires_full_placement() -> None:
    placed = Allocation(id=1, size=100, start=0, end=10, offset=0)
    unplaced = Allocation(id=2, size=100, start=0, end=10)
    with pytest.raises(ValueError, match="is not allocated"):
        validate_allocation((placed, unplaced))


def test_validate_loosened_skips_alignment_for_baseless_pools() -> None:
    pool = Pool(
        id="p", allocations=(Allocation(id=1, size=10, start=0, end=5, offset=3),)
    )
    validate_allocation(pool, require_allocated=False, alignment=8)


def test_validate_loosened_checks_alignment_for_placed_pools() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=1, size=10, start=0, end=5, offset=3),),
        offset=0,
    )
    with pytest.raises(ValueError, match="aligned"):
        validate_allocation(pool, require_allocated=False, alignment=8)
