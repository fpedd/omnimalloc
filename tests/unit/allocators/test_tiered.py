#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy import GreedyBySizeAllocator
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.tiered import TieredAllocator, TieredConfig
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _is_valid(result: tuple[Allocation, ...]) -> bool:
    return validate_allocation(Pool(id="test_pool", allocations=result))


def _spilled_bytes(result: tuple[Allocation, ...], capacity: int) -> int:
    return sum(a.size for a in result if (a.height or 0) > capacity)


def test_tiered_empty() -> None:
    assert TieredAllocator(TieredConfig(capacity=100)).allocate(()) == ()


def test_tiered_unbounded_single_at_zero() -> None:
    result = TieredAllocator().allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert result[0].offset == 0


def test_tiered_buffer_larger_than_capacity_spills() -> None:
    result = TieredAllocator(TieredConfig(capacity=50)).allocate(
        (Allocation(id=1, size=100, start=0, end=10),)
    )
    assert result[0].offset == 50


def test_tiered_non_overlapping_share_offset_on_chip() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=100, start=10, end=20),
    )
    result = TieredAllocator(TieredConfig(capacity=100)).allocate(allocs)
    by_id = {a.id: a for a in result}
    assert by_id[1].offset == 0
    assert by_id[2].offset == 0


def test_tiered_large_capacity_spills_nothing() -> None:
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    result = TieredAllocator(TieredConfig(capacity=10_000)).allocate(allocs)
    assert _spilled_bytes(result, 10_000) == 0
    assert peak_memory(result) == 500


def test_tiered_overlap_beyond_capacity_spills_above_ceiling() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=100, start=0, end=10),
    )
    result = TieredAllocator(TieredConfig(capacity=100)).allocate(allocs)
    offsets = sorted(a.offset for a in result)
    assert offsets == [0, 100]
    assert _is_valid(result)


def test_tiered_unbounded_matches_greedy_by_size() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 4 + 1) * 32, start=i, end=i + 5) for i in range(20)
    )
    tiered = TieredAllocator().allocate(allocs)
    greedy = GreedyBySizeAllocator().allocate(allocs)
    assert peak_memory(tiered) == peak_memory(greedy)


def test_tiered_invalid_capacity_raises() -> None:
    with pytest.raises(ValueError, match="capacity must be positive"):
        TieredConfig(capacity=0)


def test_tiered_invalid_order_raises() -> None:
    with pytest.raises(ValueError, match="unknown order"):
        TieredConfig(order="nope")


def test_tiered_deterministic() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i * 37 % 90) + 10, start=i % 7, end=i % 7 + 6)
        for i in range(30)
    )
    config = TieredConfig(capacity=300)
    first = TieredAllocator(config).allocate(allocs)
    second = TieredAllocator(config).allocate(allocs)
    assert [a.offset for a in first] == [a.offset for a in second]


@pytest.mark.parametrize(
    "order", ["size", "duration", "area", "conflict", "conflict_size", "start"]
)
def test_tiered_every_order_is_valid(order: str) -> None:
    allocs = tuple(
        Allocation(id=i, size=(i * 53 % 120) + 16, start=i % 9, end=i % 9 + 7)
        for i in range(40)
    )
    result = TieredAllocator(TieredConfig(capacity=400, order=order)).allocate(allocs)
    assert _is_valid(result)
    assert {a.id for a in result} == {a.id for a in allocs}


def test_tiered_spill_shrinks_monotonically_with_capacity() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i * 41 % 100) + 20, start=i % 11, end=i % 11 + 8)
        for i in range(60)
    )
    spilled = []
    for capacity in (200, 400, 600, 800, 1000):
        result = TieredAllocator(TieredConfig(capacity=capacity)).allocate(allocs)
        assert _is_valid(result)
        spilled.append(_spilled_bytes(result, capacity))
    assert spilled == sorted(spilled, reverse=True)


def test_tiered_partition_is_consistent_and_complete() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i * 29 % 80) + 24, start=i % 6, end=i % 6 + 5)
        for i in range(50)
    )
    capacity = 250
    result = TieredAllocator(TieredConfig(capacity=capacity)).allocate(allocs)
    on_chip = [a for a in result if (a.height or 0) <= capacity]
    spilled = [a for a in result if (a.height or 0) > capacity]
    assert len(on_chip) + len(spilled) == len(allocs)
    assert all(a.offset is not None and a.offset >= capacity for a in spilled)
    assert _is_valid(result)
