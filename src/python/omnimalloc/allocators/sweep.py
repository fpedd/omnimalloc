#
# SPDX-License-Identifier: Apache-2.0
#

import heapq
from typing import ClassVar

from omnimalloc._cpp import SweepFit, hybrid_sweep_place, sweep_place
from omnimalloc.primitives import Allocation

from .base import BaseAllocator
from .greedy_base import (
    allocate_parallel,
    order_by_area,
    order_by_conflict_size,
    order_by_duration,
    order_by_size,
)

# Obstacle budget for the hybrid sweep: the exact quadratic phase runs on at
# most this many allocations, keeping the overall runtime O(N log N).
DEFAULT_MAX_OBSTACLES = 1024


class SweepAllocator(BaseAllocator):
    """Chronological sweep with an address-ordered coalescing free list.

    Processes allocation/free events in time order, placing each allocation
    into the lowest free gap that fits and reusing freed space. O(N log N),
    suitable for very large problems where the greedy allocators are too slow.
    """

    _fit: ClassVar[SweepFit] = SweepFit.FIRST

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(sweep_place(allocations, self._fit))


class SweepBestFitAllocator(SweepAllocator):
    """Sweep allocator placing into the smallest fitting gap."""

    _fit = SweepFit.BEST


class SweepTwoEndedAllocator(SweepAllocator):
    """Sweep allocator: above-median sizes use first-fit, smaller best-fit."""

    _fit = SweepFit.TWO_ENDED


class HybridSweepAllocator(BaseAllocator):
    """Exact first-fit for the largest allocations, sweep for the rest.

    The top max_obstacles allocations by _rank are placed with the exact
    quadratic first-fit in _order; the remaining allocations are swept
    chronologically around them, treating the fixed placements as forbidden
    bands. O(N log N) with the default budget.
    """

    _order = staticmethod(order_by_conflict_size)

    def __init__(self, max_obstacles: int = DEFAULT_MAX_OBSTACLES) -> None:
        if max_obstacles < 0:
            raise ValueError(f"max_obstacles must be non-negative, got {max_obstacles}")
        self._max_obstacles = max_obstacles

    @staticmethod
    def _rank(allocation: Allocation) -> int:
        return allocation.size

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations
        big_ids = set(
            heapq.nlargest(
                self._max_obstacles,
                range(len(allocations)),
                key=lambda i: self._rank(allocations[i]),
            )
        )
        # Split in input order so the stable ordering tie-breaks match the
        # exact greedy variants.
        big = tuple(a for i, a in enumerate(allocations) if i in big_ids)
        rest = tuple(a for i, a in enumerate(allocations) if i not in big_ids)
        ordered = self._order(big) + rest
        return tuple(hybrid_sweep_place(ordered, len(big)))


class HybridSweepBySizeAllocator(HybridSweepAllocator):
    """Hybrid sweep ordering the exact phase by size (largest first)."""

    _order = staticmethod(order_by_size)


class HybridSweepByDurationAllocator(HybridSweepAllocator):
    """Hybrid sweep ordering the exact phase by duration (longest first)."""

    _order = staticmethod(order_by_duration)


class HybridSweepByAreaAllocator(HybridSweepAllocator):
    """Hybrid sweep selecting and ordering the exact phase by area."""

    _order = staticmethod(order_by_area)

    @staticmethod
    def _rank(allocation: Allocation) -> int:
        return allocation.size * allocation.duration


class SweepByAllAllocator(BaseAllocator):
    """Runs every sweep and hybrid sweep variant and keeps the best result."""

    def __init__(self, cores: int | None = None) -> None:
        self._cores = cores

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        variants: tuple[BaseAllocator, ...] = (
            SweepAllocator(),
            SweepBestFitAllocator(),
            SweepTwoEndedAllocator(),
            HybridSweepAllocator(),
            HybridSweepBySizeAllocator(),
            HybridSweepByDurationAllocator(),
            HybridSweepByAreaAllocator(),
        )
        return allocate_parallel(variants, allocations, cores=self._cores)
