#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import compute_temporal_overlaps, first_fit_place
from omnimalloc.primitives import Allocation

from .base import BaseAllocator
from .greedy_base import (
    allocate_parallel,
    order_by_area,
    order_by_conflict,
    order_by_conflict_size,
    order_by_duration,
    order_by_size,
    order_by_start,
)


class GreedyAllocator(BaseAllocator):
    """Base greedy allocator using first-fit strategy."""

    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        # Unbounded: placement needs the true conflict relation, never a degrade.
        overlaps = compute_temporal_overlaps(allocations, None)
        assert overlaps is not None
        return tuple(first_fit_place(allocations, overlaps))


class GreedyByDurationAllocator(GreedyAllocator):
    """Greedy allocator sorting by duration (longest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_duration(allocations))


class GreedyByConflictAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree (most conflicted first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_conflict(allocations))


class GreedyByConflictSizeAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree times size (largest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_conflict_size(allocations))


class GreedyByStartAllocator(GreedyAllocator):
    """Greedy allocator sorting by start time (earliest first, largest ties first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_start(allocations))


class GreedyByAreaAllocator(GreedyAllocator):
    """Greedy allocator sorting by area (size * duration, largest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_area(allocations))


class GreedyBySizeAllocator(GreedyAllocator):
    """Greedy allocator sorting by size (largest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_size(allocations))


class GreedyByAllAllocator(GreedyAllocator):
    """Greedy allocator that runs every variant and keeps the best result."""

    def __init__(self, cores: int | None = None) -> None:
        self._cores = cores

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        variants: tuple[BaseAllocator, ...] = (
            GreedyAllocator(),
            GreedyBySizeAllocator(),
            GreedyByDurationAllocator(),
            GreedyByAreaAllocator(),
            GreedyByConflictAllocator(),
            GreedyByConflictSizeAllocator(),
            GreedyByStartAllocator(),
        )
        return allocate_parallel(variants, allocations, cores=self._cores)
