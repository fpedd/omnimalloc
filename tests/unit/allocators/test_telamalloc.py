#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy import GreedyBySizeAllocator
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.telamalloc import TelamallocAllocator, TelamallocConfig
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _is_valid(result: tuple[Allocation, ...]) -> bool:
    return validate_allocation(Pool(id="test_pool", allocations=result))


def test_telamalloc_empty() -> None:
    allocator = TelamallocAllocator()
    result = allocator.allocate(())
    assert len(result) == 0


def test_telamalloc_single() -> None:
    allocator = TelamallocAllocator()
    alloc = Allocation(id=1, size=100, start=0, end=10)
    result = allocator.allocate((alloc,))
    assert len(result) == 1
    assert result[0].offset == 0


def test_telamalloc_rejects_negative_backtracks() -> None:
    with pytest.raises(ValueError, match="max_backtracks must be non-negative"):
        TelamallocConfig(max_backtracks=-1)


def test_telamalloc_rejects_negative_seconds() -> None:
    with pytest.raises(ValueError, match="max_seconds must be non-negative"):
        TelamallocConfig(max_seconds=-1.0)


def test_telamalloc_no_temporal_overlap_shares_offset() -> None:
    allocator = TelamallocAllocator()
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = allocator.allocate((alloc1, alloc2))
    by_id = {a.id: a for a in result}
    assert by_id[1].offset == 0
    assert by_id[2].offset == 0


def test_telamalloc_preserves_allocations() -> None:
    allocator = TelamallocAllocator()
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
    )
    result = allocator.allocate(allocs)
    assert len(result) == len(allocs)
    assert {a.id for a in result} == {1, 2}
    assert all(a.offset is not None for a in result)


def test_telamalloc_preserves_input_order() -> None:
    allocator = TelamallocAllocator()
    allocs = tuple(
        Allocation(id=i, size=(i % 3 + 1) * 10, start=0, end=10) for i in range(6)
    )
    result = allocator.allocate(allocs)
    assert [a.id for a in result] == [a.id for a in allocs]


def test_telamalloc_all_overlap_stacks_sequentially() -> None:
    allocator = TelamallocAllocator()
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = allocator.allocate(allocs)
    assert _is_valid(result)
    assert peak_memory(result) == 500


def test_telamalloc_overlapping_reach_lower_bound() -> None:
    config = TelamallocConfig(max_seconds=0)
    result = TelamallocAllocator(config).allocate(
        (
            Allocation(id=1, size=100, start=0, end=10),
            Allocation(id=2, size=50, start=5, end=15),
            Allocation(id=3, size=25, start=0, end=15),
        )
    )
    assert _is_valid(result)
    assert peak_memory(result) == 175


def test_telamalloc_independent_phases_share_address_space() -> None:
    allocator = TelamallocAllocator()
    early = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(3))
    late = tuple(Allocation(id=10 + i, size=100, start=20, end=30) for i in range(3))
    result = allocator.allocate(early + late)
    assert _is_valid(result)
    assert peak_memory(result) == 300
    offsets = sorted(a.offset for a in result if a.id >= 10)
    assert offsets == [0, 100, 200]


def test_telamalloc_deterministic_without_deadline() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 100, start=i % 3, end=i % 3 + i % 7 + 1)
        for i in range(20)
    )
    config = TelamallocConfig(max_seconds=0)
    result1 = TelamallocAllocator(config).allocate(allocs)
    result2 = TelamallocAllocator(config).allocate(allocs)
    assert all(r1.offset == r2.offset for r1, r2 in zip(result1, result2, strict=True))


def test_telamalloc_produces_valid_allocation_on_dense_overlap() -> None:
    allocator = TelamallocAllocator()
    allocs = tuple(
        Allocation(
            id=i,
            size=(i % 7 + 1) * 10,
            start=i % 4,
            end=i % 4 + (i % 3 + 1) * 3,
        )
        for i in range(30)
    )
    result = allocator.allocate(allocs)
    assert _is_valid(result)
    assert {a.id for a in result} == {a.id for a in allocs}


def test_telamalloc_matches_or_beats_single_pass_greedy() -> None:
    allocs = tuple(
        Allocation(
            id=i,
            size=(i * 37 % 50 + 1) * 10,
            start=i % 6,
            end=i % 6 + (i * 13 % 5 + 1),
        )
        for i in range(40)
    )
    greedy_peak = peak_memory(GreedyBySizeAllocator().allocate(allocs))
    config = TelamallocConfig(max_seconds=0)
    result = TelamallocAllocator(config).allocate(allocs)
    assert _is_valid(result)
    assert peak_memory(result) <= greedy_peak
