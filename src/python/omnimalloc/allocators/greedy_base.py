#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from concurrent.futures import ProcessPoolExecutor

from omnimalloc.analysis import conflict_degrees, placement_pressure
from omnimalloc.analysis.clock import uniform_dim
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.parallel import resolve_num_threads
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

    # There is nothing for a worker beyond the last variant to do, so cap on
    # both paths: an explicit count is a ceiling, not a demand for processes
    workers = min(resolve_num_threads(num_threads), len(variants))

    # One variant dying (raised, or its worker OOM-killed) must not discard
    # the placements the others already produced, on either path
    results = []
    if workers <= 1:
        for variant in variants:
            try:
                results.append(variant.allocate(allocations))
            except Exception:  # noqa: BLE001
                logger.warning("Variant %s failed; skipping it", variant, exc_info=True)
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_allocate, v, allocations) for v in variants]
            for variant, future in zip(variants, futures, strict=True):
                try:
                    results.append(future.result())
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "Variant %s failed; skipping it", variant, exc_info=True
                    )

    if not results:
        raise RuntimeError("Every allocator variant failed")
    return min(results, key=placement_pressure)
