#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

from omnimalloc.analysis import conflict_degrees, placement_pressure
from omnimalloc.analysis.clock import uniform_dim
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.parallel import (
    adopt_max_threads,
    max_threads,
    resolve_num_threads,
)
from omnimalloc.primitives import Allocation

from .base import BaseAllocator

logger = logging.getLogger(__name__)


def _sort_degrees(allocations: tuple[Allocation, ...]) -> list[int]:
    """Conflict degrees for the conflict-derived sort orders.

    Free on scalar timelines, the pruned quadratic sweep on vector clocks, so it
    carries a work budget: a truncated relation is silently a different heuristic.
    """
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


def _allocate(
    allocator: BaseAllocator, allocations: tuple[Allocation, ...]
) -> tuple[Allocation, ...]:
    """Worker: flat plain-typed kwargs make every allocator picklable."""
    return allocator.allocate(allocations)


def _run_here(
    variant: BaseAllocator, allocations: tuple[Allocation, ...]
) -> tuple[Allocation, ...] | None:
    """One variant's placement in this process, or None once it has failed."""
    try:
        return variant.allocate(allocations)
    except Exception:  # noqa: BLE001
        logger.warning("Variant %s failed; skipping it", variant, exc_info=True)
        return None


def _worker_ceiling(workers: int) -> int:
    """Thread ceiling one pool worker gets, so the pool respects the whole one.

    Workers run the native kernels too, and the ceiling is native process-global
    state a spawned worker does not inherit, so a pool hands down its share.
    """
    return max(1, max_threads() // workers)


def _run_in_pool(
    allocations: tuple[Allocation, ...],
    variants: Sequence[BaseAllocator],
    workers: int,
) -> tuple[list[tuple[Allocation, ...]], list[BaseAllocator]]:
    """Placements from the pool, plus the variants a broken pool took down.

    A worker dying abruptly (OOM kill, segfault) breaks the executor, so every
    future still in flight fails alongside it. Those variants never produced an
    answer at all, and come back for a retry rather than counting as failures.
    """
    results = []
    stranded = []
    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=adopt_max_threads,
        initargs=(_worker_ceiling(workers),),
    ) as pool:
        futures = [pool.submit(_allocate, v, allocations) for v in variants]
        for variant, future in zip(variants, futures, strict=True):
            try:
                results.append(future.result())
            except BrokenProcessPool:
                stranded.append(variant)
            except Exception:  # noqa: BLE001
                logger.warning("Variant %s failed; skipping it", variant, exc_info=True)
    return results, stranded


def _rerun_stranded(
    allocations: tuple[Allocation, ...], stranded: list[BaseAllocator]
) -> list[tuple[Allocation, ...]]:
    """Retry the variants a broken pool took down, one pool each.

    One at a time, and never in this process: whatever killed the worker would
    kill the caller too, and a shared retry pool would just strand them again.
    """
    logger.warning("Worker pool broke; retrying %d variant(s)", len(stranded))
    results = []
    for variant in stranded:
        placed, failed = _run_in_pool(allocations, [variant], workers=1)
        results += placed
        if failed:
            logger.warning("Variant %s took its worker down; skipping it", variant)
    return results


def allocate_parallel(
    allocations: tuple[Allocation, ...],
    variants: tuple[BaseAllocator, ...],
    num_threads: int | None = None,
) -> tuple[Allocation, ...]:
    """Run each variant and return the lowest peak memory results.

    `num_threads=None` uses every worker the thread cap allows. A failing
    variant is logged and dropped; only an all-failing set raises.
    """

    if not allocations:
        return allocations

    workers = resolve_num_threads(num_threads)
    if num_threads is None:
        workers = min(workers, len(variants))

    if workers <= 1:
        placements = (_run_here(variant, allocations) for variant in variants)
        results = [placed for placed in placements if placed is not None]
    else:
        results, stranded = _run_in_pool(allocations, variants, workers)
        if stranded:
            results += _rerun_stranded(allocations, stranded)

    if not results:
        raise RuntimeError("Every allocator variant failed")
    return min(results, key=placement_pressure)
