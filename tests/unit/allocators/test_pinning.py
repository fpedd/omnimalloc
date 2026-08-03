#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators import BaseAllocator, available_allocators
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.allocators.telamalloc import TelamallocAllocator
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _supporting_allocators() -> list[BaseAllocator]:
    instances = []
    for name in available_allocators():
        allocator = BaseAllocator.get(name)
        if not allocator.supports_pinned:
            continue
        try:
            instances.append(allocator())
        except ImportError:
            continue
    return instances


def test_pin_survives_every_supporting_allocator() -> None:
    allocations = (
        Allocation(id="pinned", size=10, start=0, end=5, offset=100),
        Allocation(id="free", size=10, start=0, end=5),
    )
    for allocator in _supporting_allocators():
        placed = {a.id: a.offset for a in allocator.allocate(allocations)}
        assert placed["pinned"] == 100, allocator
        assert placed["free"] != 100, allocator


def test_allocator_without_pin_support_rejects_pins() -> None:
    allocations = (Allocation(id=1, size=10, start=0, end=5, offset=0),)
    with pytest.raises(ValueError, match="cannot honor pinned offsets"):
        TelamallocAllocator().allocate(allocations)


def test_allocator_without_pin_support_accepts_free_allocations() -> None:
    allocations = (Allocation(id=1, size=10, start=0, end=5),)
    assert TelamallocAllocator().allocate(allocations)[0].offset == 0


def test_pin_blocks_the_address_of_a_conflicting_allocation() -> None:
    allocations = (
        Allocation(id="pinned", size=10, start=0, end=5, offset=0),
        Allocation(id="free", size=10, start=2, end=7),
    )
    placed = {a.id: a.offset for a in OmniAllocator().allocate(allocations)}
    assert placed == {"pinned": 0, "free": 10}


def test_free_allocation_reuses_a_pinned_address_when_lifetimes_are_disjoint() -> None:
    allocations = (
        Allocation(id="pinned", size=10, start=0, end=5, offset=0),
        Allocation(id="free", size=10, start=5, end=9),
    )
    placed = {a.id: a.offset for a in OmniAllocator().allocate(allocations)}
    assert placed == {"pinned": 0, "free": 0}


def test_colliding_pins_are_rejected() -> None:
    allocations = (
        Allocation(id="a", size=10, start=0, end=5, offset=0),
        Allocation(id="b", size=10, start=1, end=6, offset=5),
    )
    with pytest.raises(ValueError, match="'a' and 'b' already collide"):
        OmniAllocator().allocate(allocations)


def test_fully_pinned_input_comes_back_unchanged() -> None:
    allocations = tuple(
        Allocation(id=i, size=10, start=0, end=5, offset=10 * i) for i in range(5)
    )
    assert OmniAllocator().allocate(allocations) == allocations


def test_pin_is_honored_with_vector_clocks() -> None:
    allocations = (
        Allocation(id="pinned", size=10, start=(0, 0), end=(4, 1), offset=64),
        Allocation(id="free", size=10, start=(0, 0), end=(1, 4)),
    )
    placed = {a.id: a.offset for a in OmniAllocator().allocate(allocations)}
    assert placed["pinned"] == 64
    assert placed["free"] == 0


def test_pinned_placement_validates() -> None:
    allocations = (
        Allocation(id="pinned", size=64, start=0, end=10, offset=1024),
        *(Allocation(id=i, size=32, start=i, end=i + 4) for i in range(20)),
    )
    placed = OmniAllocator().allocate(allocations)
    validate_allocation(Pool(id="p", allocations=placed))


def test_naive_fills_the_gap_below_a_pin() -> None:
    allocations = (
        Allocation(id="pinned", size=10, start=0, end=5, offset=50),
        Allocation(id="small", size=20, start=0, end=5),
        Allocation(id="large", size=90, start=0, end=5),
    )
    placed = {a.id: a.offset for a in NaiveAllocator().allocate(allocations)}
    assert placed == {"pinned": 50, "small": 0, "large": 60}


def test_staged_allocation_extends_an_earlier_placement() -> None:
    first = OmniAllocator().allocate(
        tuple(Allocation(id=i, size=16, start=i, end=i + 3) for i in range(8))
    )
    extra = tuple(Allocation(id=100 + i, size=24, start=i, end=i + 5) for i in range(4))
    second = OmniAllocator().allocate((*first, *extra))
    assert {a.id: a.offset for a in second[:8]} == {a.id: a.offset for a in first}
    validate_allocation(Pool(id="p", allocations=second))
