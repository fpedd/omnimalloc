#
# SPDX-License-Identifier: Apache-2.0
#

from concurrent.futures import ProcessPoolExecutor

from omnimalloc.analysis import conflict_degrees, placement_pressure
from omnimalloc.analysis.clock import uniform_dim
from omnimalloc.common.parallel import resolve_num_threads
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


def _unbudgeted_degrees(allocations: tuple[Allocation, ...]) -> list[int]:
    # Unbounded: the sort order must not degrade on large instances.
    return conflict_degrees(allocations, work_budget=None)


def order_by_size(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by size (largest first)."""
    return tuple(sorted(allocations, key=lambda a: a.size, reverse=True))


def order_by_duration(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by duration (longest first)."""
    return tuple(sorted(allocations, key=lambda a: a.duration, reverse=True))


def order_by_area(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by area (size * duration, largest first)."""
    return tuple(sorted(allocations, key=lambda a: a.area, reverse=True))


def order_by_conflict(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by conflict degree (most conflicted first)."""
    degrees = _unbudgeted_degrees(allocations)
    paired = sorted(
        zip(allocations, degrees, strict=True),
        key=lambda pair: (pair[1], pair[0].size),
        reverse=True,
    )
    return tuple(alloc for alloc, _ in paired)


def order_by_conflict_size(
    allocations: tuple[Allocation, ...],
) -> tuple[Allocation, ...]:
    """Order by conflict degree times size (largest first)."""
    degrees = _unbudgeted_degrees(allocations)
    paired = sorted(
        zip(allocations, degrees, strict=True),
        key=lambda pair: (pair[1] * pair[0].size, pair[0].size),
        reverse=True,
    )
    return tuple(alloc for alloc, _ in paired)


def order_by_start(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by start time (earliest first, largest ties first)."""
    uniform_dim(allocations)  # mixed scalar/tuple starts do not compare
    return tuple(sorted(allocations, key=lambda a: (a.start, -a.size)))


def _allocate(
    allocator: BaseAllocator, allocations: tuple[Allocation, ...]
) -> tuple[Allocation, ...]:
    """Worker: flat plain-typed kwargs make every allocator picklable."""
    return allocator.allocate(allocations)


def allocate_parallel(
    allocations: tuple[Allocation, ...],
    variants: tuple[BaseAllocator, ...],
    *,
    num_threads: int | None = None,
) -> tuple[Allocation, ...]:
    """Run each variant and return the lowest peak memory results.

    `num_threads=None` uses all cores (capped at the variant count).
    """

    if not allocations:
        return allocations

    workers = resolve_num_threads(num_threads)
    if num_threads is None:
        workers = min(workers, len(variants))
    if workers <= 1:
        return min((v.allocate(allocations) for v in variants), key=placement_pressure)

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_allocate, v, allocations) for v in variants]
        return min((future.result() for future in futures), key=placement_pressure)
