#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.hillclimb import HillClimbAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation


def _is_valid(result: tuple[Allocation, ...]) -> bool:
    return validate_allocation(Pool(id="test_pool", allocations=result))


def test_hillclimb_empty() -> None:
    allocator = HillClimbAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


def test_hillclimb_single() -> None:
    allocator = HillClimbAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0


def test_hillclimb_rejects_positive_iterations() -> None:
    with pytest.raises(ValueError, match="max_iterations must be positive"):
        HillClimbAllocator(max_iterations=0)


def test_hillclimb_rejects_negative_temperature() -> None:
    with pytest.raises(ValueError, match="acceptance_temperature must be non-negative"):
        HillClimbAllocator(acceptance_temperature=-1.0)


def test_hillclimb_rejects_negative_timeout() -> None:
    with pytest.raises(ValueError, match="timeout must be positive or None"):
        HillClimbAllocator(timeout=-1.0)


def test_hillclimb_preserves_allocations() -> None:
    allocator = HillClimbAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = allocator.allocate(allocs)
    assert len(result) == len(allocs)
    assert {a.id for a in result} == {1, 2}
    assert all(a.offset is not None for a in result)


def test_hillclimb_produces_valid_allocation() -> None:
    allocator = HillClimbAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=5),
        Allocation(id=2, size=100, start=3, end=8),
        Allocation(id=3, size=100, start=6, end=10),
        Allocation(id=4, size=50, start=0, end=10),
    )
    result = allocator.allocate(allocs)
    assert _is_valid(result)


def test_hillclimb_no_temporal_overlap_shares_offset() -> None:
    allocator = HillClimbAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    by_id = {a.id: a for a in result}
    assert by_id[1].offset == 0
    assert by_id[2].offset == 0


def test_hillclimb_all_overlap_stacks_sequentially() -> None:
    allocator = HillClimbAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    assert _is_valid(result)
    assert peak_memory(result) == 500


def test_hillclimb_survives_rejected_step_undo() -> None:
    allocator = HillClimbAllocator(max_iterations=6)
    allocs = (
        Allocation(id=0, size=50, start=4, end=7),
        Allocation(id=1, size=40, start=2, end=6),
        Allocation(id=2, size=30, start=2, end=4),
        Allocation(id=3, size=40, start=4, end=5),
        Allocation(id=4, size=70, start=2, end=3),
    )
    result = allocator.allocate(allocs)
    assert len(result) == len(allocs)
    assert _is_valid(result)


def test_hillclimb_deterministic() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=i % 3, end=i % 3 + i % 7 + 1)
        for i in range(20)
    )
    result1 = HillClimbAllocator(max_iterations=50).allocate(allocs)
    result2 = HillClimbAllocator(max_iterations=50).allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_hillclimb_not_worse_than_greedy_by_size() -> None:
    from omnimalloc.allocators.greedy import GreedyBySizeAllocator

    allocs = tuple(
        Allocation(
            id=i,
            size=(i * 37 % 90 + 10),
            start=(i * 13) % 40,
            end=(i * 13) % 40 + (i * 7) % 15 + 1,
        )
        for i in range(40)
    )
    hillclimb = HillClimbAllocator(seed=42).allocate(allocs)
    greedy = GreedyBySizeAllocator().allocate(allocs)
    assert _is_valid(hillclimb)
    assert peak_memory(hillclimb) <= peak_memory(greedy)
