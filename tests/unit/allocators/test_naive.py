#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation


def test_naive_empty() -> None:
    result = NaiveAllocator().allocate(())
    assert len(result) == 0


def test_naive_single() -> None:
    result = NaiveAllocator().allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert len(result) == 1
    assert result[0].offset == 0


def test_naive_stacks_sequentially() -> None:
    allocs = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(4))
    result = NaiveAllocator().allocate(allocs)
    assert [a.offset for a in result] == [0, 100, 200, 300]


def test_naive_does_not_reuse_freed_space() -> None:
    alloc1 = Allocation(id=1, size=100, start=0, end=10)
    alloc2 = Allocation(id=2, size=200, start=10, end=20)
    result = NaiveAllocator().allocate((alloc1, alloc2))
    assert result[0].offset == 0
    assert result[1].offset == 100


def test_naive_preserves_input_order_and_ids() -> None:
    allocs = (
        Allocation(id="b", size=50, start=0, end=5),
        Allocation(id="a", size=100, start=5, end=10),
    )
    result = NaiveAllocator().allocate(allocs)
    assert [a.id for a in result] == ["b", "a"]


def test_naive_mixed_sizes_cumulative_offsets() -> None:
    sizes = (10, 250, 3, 42)
    allocs = tuple(
        Allocation(id=i, size=size, start=i, end=i + 2) for i, size in enumerate(sizes)
    )
    result = NaiveAllocator().allocate(allocs)
    assert [a.offset for a in result] == [0, 10, 260, 263]
