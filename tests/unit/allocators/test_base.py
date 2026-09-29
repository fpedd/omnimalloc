#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.primitives import Allocation

ALLOCATIONS = tuple(Allocation(id=i, size=10 + i, start=i, end=i + 3) for i in range(6))


class TruncatingAllocator(BaseAllocator):
    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(a.with_offset(0) for a in allocations[:1])


class PaddingAllocator(BaseAllocator):
    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        placed = tuple(a.with_offset(0) for a in allocations)
        return (*placed, placed[0])


class UnplacingAllocator(BaseAllocator):
    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return allocations


class ReversingAllocator(BaseAllocator):
    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(a.with_offset(0) for a in reversed(allocations))


def test_allocator_leaving_an_allocation_unplaced_is_rejected() -> None:
    with pytest.raises(ValueError, match="left allocation 0 unplaced"):
        UnplacingAllocator().allocate(ALLOCATIONS)


def test_allocator_moving_a_pin_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patched rather than subclassed: a registered pin-mover would join the
    # registry-wide pinning checks
    def move(_: NaiveAllocator, allocations: tuple[Allocation, ...]) -> tuple:
        return tuple(a.with_offset(1000) for a in allocations)

    monkeypatch.setattr(NaiveAllocator, "_allocate", move)
    pinned = (ALLOCATIONS[0].with_offset(0), *ALLOCATIONS[1:])
    with pytest.raises(ValueError, match="moved pinned allocation 0 from 0 to 1000"):
        NaiveAllocator().allocate(pinned)


def test_allocate_restores_input_order() -> None:
    placed = ReversingAllocator().allocate(ALLOCATIONS)
    assert [a.id for a in placed] == [a.id for a in ALLOCATIONS]


def test_allocate_of_nothing_returns_nothing() -> None:
    assert UnplacingAllocator().allocate(()) == ()


def test_allocator_dropping_allocations_is_rejected() -> None:
    with pytest.raises(ValueError, match="returned a different allocation set"):
        TruncatingAllocator().allocate(ALLOCATIONS)


def test_allocator_duplicating_allocations_is_rejected() -> None:
    with pytest.raises(ValueError, match="returned a different allocation set"):
        PaddingAllocator().allocate(ALLOCATIONS)


def test_a_faithful_allocator_is_accepted() -> None:
    placed = OmniAllocator().allocate(ALLOCATIONS)
    assert {a.id for a in placed} == {a.id for a in ALLOCATIONS}


def test_supports_counts_pins_like_ensure_supported() -> None:
    pinned = (Allocation(id=1, size=4, start=0, end=2, offset=0),)
    assert OmniAllocator().supports(pinned) is True
    assert TruncatingAllocator().supports(pinned) is False
    with pytest.raises(ValueError, match="cannot honor pinned offsets"):
        TruncatingAllocator().allocate(pinned)
