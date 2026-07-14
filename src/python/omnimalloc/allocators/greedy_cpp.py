#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import GreedyAllocatorCpp as _GreedyAllocatorCpp
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


class GreedyAllocatorCpp(BaseAllocator):
    """C++ implementation of the base greedy allocator using first-fit strategy."""

    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(_GreedyAllocatorCpp().allocate(list(allocations)))


class GreedyByDurationAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by duration (longest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_duration(allocations))


class GreedyByConflictAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by conflict degree (most conflicted first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_conflict(allocations))


class GreedyByConflictSizeAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by conflict degree times size (largest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_conflict_size(allocations))


class GreedyByStartAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by start time (earliest, largest ties first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_start(allocations))


class GreedyByAreaAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by area (size * duration, largest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_area(allocations))


class GreedyBySizeAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by size (largest first)."""

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return super()._allocate(order_by_size(allocations))


class GreedyByAllAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator that runs every variant and keeps the best result."""

    def __init__(self, cores: int | None = None) -> None:
        self._cores = cores

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        variants: tuple[BaseAllocator, ...] = (
            GreedyAllocatorCpp(),
            GreedyBySizeAllocatorCpp(),
            GreedyByDurationAllocatorCpp(),
            GreedyByAreaAllocatorCpp(),
            GreedyByConflictAllocatorCpp(),
            GreedyByConflictSizeAllocatorCpp(),
            GreedyByStartAllocatorCpp(),
        )
        return allocate_parallel(variants, allocations, cores=self._cores)
