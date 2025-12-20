#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class GreedyAllocator(BaseAllocator):
    """Base greedy allocator using first-fit strategy."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        placed_allocations: list[Allocation] = []

        for current_alloc in allocations:
            # Collect overlapping allocations sorted by offset
            overlapping = [
                placed
                for placed in placed_allocations
                if current_alloc.overlaps_temporally(placed)
            ]
            overlapping.sort(key=lambda a: a.offset or 0, reverse=False)

            # Find offset using first-fit (outperforms best-fit in practice)
            best_offset = 0
            for placed in overlapping:
                assert placed.offset is not None
                gap = placed.offset - best_offset
                if gap >= current_alloc.size:
                    break
                best_offset = max(best_offset, placed.offset + placed.size)

            new_alloc = current_alloc.with_offset(best_offset)
            placed_allocations.append(new_alloc)

        return tuple(placed_allocations)


class GreedyByDurationAllocator(GreedyAllocator):
    """Greedy allocator sorting by duration (longest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(allocations, key=lambda a: a.duration, reverse=True)
        return super().allocate(tuple(sorted_allocs))


class GreedyByConflictAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree (most conflicted first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        conflict_degrees = {}
        for alloc in allocations:
            conflicts = sum(
                1
                for other in allocations
                if other != alloc and alloc.overlaps_temporally(other)
            )
            conflict_degrees[alloc] = conflicts

        sorted_allocs = sorted(
            allocations, key=lambda a: (conflict_degrees[a], a.size), reverse=True
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyByAreaAllocator(GreedyAllocator):
    """Greedy allocator sorting by area (size * duration, largest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(
            allocations, key=lambda a: a.size * a.duration, reverse=True
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyBySizeAllocator(GreedyAllocator):
    """Greedy allocator sorting by size (largest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(allocations, key=lambda a: a.size, reverse=True)
        return super().allocate(tuple(sorted_allocs))
