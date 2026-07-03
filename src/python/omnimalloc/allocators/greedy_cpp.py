#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import GreedyAllocatorCpp as _GreedyAllocatorCpp
from omnimalloc.primitives import Allocation

from .base import BaseAllocator
from .greedy import allocate_parallel, compute_conflict_degrees


class GreedyAllocatorCpp(BaseAllocator):
    """C++ implementation of the base greedy allocator using first-fit strategy."""

    def __init__(self) -> None:
        self._cpp_allocator = _GreedyAllocatorCpp()

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        allocs_list = list(allocations)
        result = self._cpp_allocator.allocate(allocs_list)
        return tuple(result)


class GreedyByDurationAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by duration (longest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(allocations, key=lambda a: a.duration, reverse=True)
        return super().allocate(tuple(sorted_allocs))


class GreedyByConflictAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by conflict degree (most conflicted first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        conflict_degrees = compute_conflict_degrees(allocations)
        sorted_allocs = sorted(
            allocations, key=lambda a: (conflict_degrees[a], a.size), reverse=True
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyByConflictSizeAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by conflict degree times size (largest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        conflict_degrees = compute_conflict_degrees(allocations)
        sorted_allocs = sorted(
            allocations,
            key=lambda a: (conflict_degrees[a] * a.size, a.size),
            reverse=True,
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyByStartAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by start time (earliest, largest ties first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(allocations, key=lambda a: (a.start, -a.size))
        return super().allocate(tuple(sorted_allocs))


class GreedyByAreaAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by area (size * duration, largest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(
            allocations, key=lambda a: a.size * a.duration, reverse=True
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyBySizeAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator sorting by size (largest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(allocations, key=lambda a: a.size, reverse=True)
        return super().allocate(tuple(sorted_allocs))


class GreedyByAllAllocatorCpp(GreedyAllocatorCpp):
    """C++ greedy allocator that runs every variant and keeps the best result."""

    def __init__(self, cores: int | None = None) -> None:
        super().__init__()
        self._cores = cores

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
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
