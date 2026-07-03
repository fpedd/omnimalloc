#
# SPDX-License-Identifier: Apache-2.0
#

import os
from bisect import bisect_left, bisect_right
from concurrent.futures import ProcessPoolExecutor

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


def compute_conflict_degrees(
    allocations: tuple[Allocation, ...],
) -> dict[Allocation, int]:
    """Count temporally overlapping allocations for each allocation."""
    starts = sorted(alloc.start for alloc in allocations)
    ends = sorted(alloc.end for alloc in allocations)
    # An allocation overlaps [start, end) iff it starts before `end` and does
    # not end by `start`; subtract 1 so the allocation does not count itself.
    return {
        alloc: bisect_left(starts, alloc.end) - bisect_right(ends, alloc.start) - 1
        for alloc in allocations
    }


class GreedyByConflictAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree (most conflicted first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        conflict_degrees = compute_conflict_degrees(allocations)
        sorted_allocs = sorted(
            allocations, key=lambda a: (conflict_degrees[a], a.size), reverse=True
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyByConflictSizeAllocator(GreedyAllocator):
    """Greedy allocator sorting by conflict degree times size (largest first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        conflict_degrees = compute_conflict_degrees(allocations)
        sorted_allocs = sorted(
            allocations,
            key=lambda a: (conflict_degrees[a] * a.size, a.size),
            reverse=True,
        )
        return super().allocate(tuple(sorted_allocs))


class GreedyByStartAllocator(GreedyAllocator):
    """Greedy allocator sorting by start time (earliest first, largest ties first)."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        sorted_allocs = sorted(allocations, key=lambda a: (a.start, -a.size))
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


def _allocate(name: str, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Worker: rebuild the named allocator from the registry and run it."""
    return BaseAllocator.get(name)().allocate(allocations)


def allocate_parallel(
    variants: tuple[BaseAllocator, ...],
    allocations: tuple[Allocation, ...],
    cores: int | None = None,
) -> tuple[Allocation, ...]:
    """Run each variant and return the lowest peak memory results."""

    if not allocations:
        return allocations

    def peak(result: tuple[Allocation, ...]) -> int:
        return max((a.height for a in result if a.height is not None), default=0)

    workers = cores if cores is not None else min(os.cpu_count() or 1, len(variants))
    if workers <= 1:
        return min((v.allocate(allocations) for v in variants), key=peak)

    def config(a: BaseAllocator) -> dict[str, object]:
        plain = (bool, int, float, str, tuple, type(None))
        return {k: v for k, v in vars(a).items() if isinstance(v, plain)}

    for variant in variants:
        if config(variant) != config(type(variant)()):
            raise ValueError(f"variant '{variant.name()}' must be default-configured")

    names = [variant.name() for variant in variants]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_allocate, name, allocations) for name in names]
        return min((future.result() for future in futures), key=peak)


class GreedyByAllAllocator(GreedyAllocator):
    """Greedy allocator that runs every variant and keeps the best result."""

    def __init__(self, cores: int | None = None) -> None:
        self._cores = cores

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
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
