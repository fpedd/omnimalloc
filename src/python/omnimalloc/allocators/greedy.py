#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from concurrent.futures import ThreadPoolExecutor

from omnimalloc._cpp import first_fit_place
from omnimalloc.analysis import conflict_degrees, placement_pressure
from omnimalloc.analysis._clock import time_components, uniform_dim
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
    dim = uniform_dim(allocations)  # mixed scalar/tuple starts do not compare
    starts = [time_components(a.start) for a in allocations]
    ends = [time_components(a.end) for a in allocations]

    # Ordering lanes by their own contents stops lane labelling from deciding
    # the packing; mirrors canonical_starts in first_fit.cpp
    lanes = sorted(
        range(dim),
        key=lambda lane: sorted(
            (s[lane], e[lane]) for s, e in zip(starts, ends, strict=True)
        ),
    )
    order = sorted(
        range(len(allocations)),
        key=lambda i: (
            tuple(starts[i][lane] for lane in lanes),
            -allocations[i].size,
            str(allocations[i].id),
        ),
    )
    return tuple(allocations[i] for i in order)


def allocate_parallel(
    allocations: tuple[Allocation, ...],
    variants: tuple[BaseAllocator, ...],
    num_threads: int | None = None,
) -> tuple[Allocation, ...]:
    """Run each variant and return the lowest peak memory results."""

    if not allocations:
        return allocations

    if not variants:
        raise ValueError("No allocator variants to run")

    workers = min(resolve_num_threads(num_threads), len(variants))

    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(v.allocate, allocations) for v in variants]
        for variant, future in zip(variants, futures, strict=True):
            try:
                results.append(future.result())
            except Exception:
                logger.warning("Variant %s failed; skipping it", variant, exc_info=True)

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
