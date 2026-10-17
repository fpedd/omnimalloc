#
# SPDX-License-Identifier: Apache-2.0
#

from typing import ClassVar

from omnimalloc._cpp import GreedyOrder, greedy_order, greedy_place
from omnimalloc.analysis import try_linearize
from omnimalloc.analysis._clock import uniform_dim
from omnimalloc.primitives import Allocation

from .base import BaseAllocator
from .omni import OmniAllocator


def _ordered(
    allocations: tuple[Allocation, ...],
    order: GreedyOrder,
    line: tuple[Allocation, ...] | None = None,
) -> tuple[Allocation, ...]:
    return tuple(allocations[i] for i in greedy_order(line or allocations, order))


def _linearized(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...] | None:
    """Scalar surrogate of vector-clock lifetimes, or None if scalar or unavailable."""
    return None if uniform_dim(allocations) == 1 else try_linearize(allocations)


def order_by_size(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by size (largest first)."""
    return _ordered(allocations, GreedyOrder.SIZE)


def order_by_duration(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by duration (longest first)."""
    return _ordered(allocations, GreedyOrder.DURATION)


def order_by_area(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by area (size * duration, largest first)."""
    return _ordered(allocations, GreedyOrder.AREA)


def order_by_conflict(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by conflict degree (most conflicted first)."""
    return _ordered(allocations, GreedyOrder.CONFLICT)


def order_by_conflict_size(
    allocations: tuple[Allocation, ...],
) -> tuple[Allocation, ...]:
    """Order by conflict degree times size (largest first)."""
    return _ordered(allocations, GreedyOrder.CONFLICT_SIZE)


def order_by_start(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by start time (earliest first, largest ties first)."""
    return _ordered(allocations, GreedyOrder.START)


def order_by_contention(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by lifetime peak, duration, size; input order unless linearizable."""
    return _ordered(allocations, GreedyOrder.CONTENTION, _linearized(allocations))


class GreedyAllocator(BaseAllocator):
    """First-fit placement in `order`, ties kept in input order."""

    supports_vector_time = True
    supports_pinned = True

    order: ClassVar[GreedyOrder] = GreedyOrder.INPUT

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(greedy_place(allocations, self.order))


class GreedyBySizeAllocator(GreedyAllocator):
    """Greedy allocator sorting by size (largest first)."""

    order = GreedyOrder.SIZE


class GreedyByDurationAllocator(GreedyAllocator):
    """Greedy allocator sorting by duration (longest first)."""

    order = GreedyOrder.DURATION


class GreedyByAreaAllocator(GreedyAllocator):
    """Greedy allocator sorting by area (size * duration, largest first)."""

    order = GreedyOrder.AREA


class GreedyByConflictAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree (most conflicted first)."""

    order = GreedyOrder.CONFLICT


class GreedyByConflictSizeAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree times size (largest first)."""

    order = GreedyOrder.CONFLICT_SIZE


class GreedyByStartAllocator(GreedyAllocator):
    """Greedy allocator sorting by start time (earliest first, largest ties first)."""

    order = GreedyOrder.START


class GreedyByContentionAllocator(GreedyAllocator):
    """Greedy allocator sorting by peak pressure over the lifetime (highest first)."""

    order = GreedyOrder.CONTENTION

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        line = _linearized(allocations)
        if line is None:
            return super()._allocate(allocations)
        # The surrogate shares the conflict relation, so its offsets transfer
        placed = greedy_place(line, self.order)
        return tuple(
            a.with_offset(p.offset) for a, p in zip(allocations, placed, strict=True)
        )


class GreedyByAllAllocator(OmniAllocator):
    """Every greedy order raced for the lowest peak: omni without linearization."""

    def __init__(self) -> None:
        super().__init__(linearize_budget=0)
