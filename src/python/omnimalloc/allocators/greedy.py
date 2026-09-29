#
# SPDX-License-Identifier: Apache-2.0
#

from typing import ClassVar

from omnimalloc._cpp import GreedyOrder, greedy_place
from omnimalloc.primitives import Allocation

from .base import BaseAllocator
from .omni import OmniAllocator


class GreedyAllocator(BaseAllocator):
    """First-fit placement in input order; subclasses set another `order`.

    Ties in every order keep input order.
    """

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


class GreedyByAllAllocator(OmniAllocator):
    """Every greedy order raced for the lowest peak: omni without linearization."""

    def __init__(self) -> None:
        super().__init__(linearize_budget=0)
