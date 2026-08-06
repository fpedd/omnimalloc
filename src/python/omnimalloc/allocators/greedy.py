#
# SPDX-License-Identifier: Apache-2.0
#

import logging
import threading
from concurrent.futures import ProcessPoolExecutor

from omnimalloc._cpp import first_fit_place
from omnimalloc.analysis import conflict_degrees, placement_pressure
from omnimalloc.analysis.clock import uniform_dim
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.parallel import resolve_num_threads
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import Allocation

from .base import BaseAllocator

logger = logging.getLogger(__name__)


def _sort_degrees(allocations: tuple[Allocation, ...]) -> list[int]:
    """Conflict degrees for the conflict-derived sort orders."""
    return conflict_degrees(allocations, work_budget=DEFAULT_WORK_BUDGET)


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
    degrees = _sort_degrees(allocations)
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
    degrees = _sort_degrees(allocations)
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


def _run_variant(
    allocator: BaseAllocator, allocations: tuple[Allocation, ...]
) -> tuple[Allocation, ...]:
    """Worker: flat plain-typed kwargs make every allocator picklable."""
    return allocator.allocate(allocations)


def _place_serially(
    allocations: tuple[Allocation, ...], variants: tuple[BaseAllocator, ...]
) -> list[tuple[Allocation, ...]]:
    results = []
    for variant in variants:
        try:
            results.append(variant.allocate(allocations))
        except Exception:
            logger.warning("Variant %s failed; skipping it", variant, exc_info=True)
    return results


def _pool_is_safe(workers: int) -> bool:
    """Whether the pool is worth starting for this many workers.

    It forks, and forking a process that has other threads running leaves the
    child holding locks nobody will release: it may raise, or it may deadlock.
    """
    return workers > 1 and threading.active_count() == 1


def _place_pooled(
    allocations: tuple[Allocation, ...],
    variants: tuple[BaseAllocator, ...],
    workers: int,
) -> list[tuple[Allocation, ...]] | None:
    """Results from the process pool, or None when the fork is refused.

    The backstop for a thread that starts between the check and the fork; a
    placement is worth more than the parallelism, so the caller retries serially.
    """
    results = []
    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_run_variant, v, allocations) for v in variants]
            for variant, future in zip(variants, futures, strict=True):
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning(
                        "Variant %s failed; skipping it", variant, exc_info=True
                    )
    except (OSError, RuntimeError) as e:
        logger.warning("Process pool unavailable (%s); placing serially", e)
        return None
    return results


def allocate_parallel(
    allocations: tuple[Allocation, ...],
    variants: tuple[BaseAllocator, ...],
    num_threads: int | None = None,
) -> tuple[Allocation, ...]:
    """Run each variant and return the lowest peak memory results."""

    if not allocations:
        return allocations

    workers = min(resolve_num_threads(num_threads), len(variants))

    results = (
        _place_pooled(allocations, variants, workers)
        if _pool_is_safe(workers)
        else None
    )
    if results is None:
        results = _place_serially(allocations, variants)

    if not results:
        raise RuntimeError("Every allocator variant failed")
    return min(results, key=placement_pressure)


class GreedyAllocator(BaseAllocator):
    """Base greedy allocator using first-fit strategy."""

    supports_vector_time = True
    supports_pinned = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(first_fit_place(allocations))


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

    def __init__(self, num_threads: int | None = None) -> None:
        ensure_positive(num_threads, "num_threads", allow_none=True)
        self._num_threads = num_threads

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
        return allocate_parallel(allocations, variants, num_threads=self._num_threads)
