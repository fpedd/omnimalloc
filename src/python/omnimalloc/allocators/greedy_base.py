#
# SPDX-License-Identifier: Apache-2.0
#

import dataclasses
import os
from bisect import bisect_left, bisect_right
from concurrent.futures import ProcessPoolExecutor

from omnimalloc.primitives import Allocation

from .base import BaseAllocator


def peak_memory(allocations: tuple[Allocation, ...]) -> int:
    """Return the highest end offset across the allocated allocations."""
    return max((a.height for a in allocations if a.height is not None), default=0)


def compute_conflicts(allocations: tuple[Allocation, ...]) -> dict[Allocation, int]:
    """Count temporally overlapping allocations for each allocation."""
    starts = sorted(alloc.start for alloc in allocations)
    ends = sorted(alloc.end for alloc in allocations)
    # An allocation overlaps [start, end) iff it starts before `end` and does
    # not end by `start`; subtract 1 so the allocation does not count itself.
    return {
        alloc: bisect_left(starts, alloc.end) - bisect_right(ends, alloc.start) - 1
        for alloc in allocations
    }


def order_by_size(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by size (largest first)."""
    return tuple(sorted(allocations, key=lambda a: a.size, reverse=True))


def order_by_duration(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by duration (longest first)."""
    return tuple(sorted(allocations, key=lambda a: a.duration, reverse=True))


def order_by_area(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by area (size * duration, largest first)."""
    return tuple(sorted(allocations, key=lambda a: a.size * a.duration, reverse=True))


def order_by_conflict(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by conflict degree (most conflicted first)."""
    conflicts = compute_conflicts(allocations)
    return tuple(
        sorted(allocations, key=lambda a: (conflicts[a], a.size), reverse=True)
    )


def order_by_conflict_size(
    allocations: tuple[Allocation, ...],
) -> tuple[Allocation, ...]:
    """Order by conflict degree times size (largest first)."""
    conflicts = compute_conflicts(allocations)
    return tuple(
        sorted(allocations, key=lambda a: (conflicts[a] * a.size, a.size), reverse=True)
    )


def order_by_start(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by start time (earliest first, largest ties first)."""
    return tuple(sorted(allocations, key=lambda a: (a.start, -a.size)))


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

    workers = cores if cores is not None else min(os.cpu_count() or 1, len(variants))
    if workers <= 1:
        return min((v.allocate(allocations) for v in variants), key=peak_memory)

    def config(a: BaseAllocator) -> dict[str, object]:
        # Frozen config dataclasses compare by value, so include them alongside
        # plain types; anything else (caches, C++ handles) is not config.
        plain = (bool, int, float, str, tuple, type(None))
        return {
            k: v
            for k, v in vars(a).items()
            if isinstance(v, plain) or dataclasses.is_dataclass(v)
        }

    for variant in variants:
        if config(variant) != config(type(variant)()):
            raise ValueError(f"variant '{variant.name()}' must be default-configured")

    names = [variant.name() for variant in variants]
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_allocate, name, allocations) for name in names]
        return min((future.result() for future in futures), key=peak_memory)
