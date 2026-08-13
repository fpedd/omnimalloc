#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

from omnimalloc._cpp import FirstFitPlacer, first_fit_place
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


def _conflict_load(_alloc: Allocation, degree: int) -> int:
    return degree


def _conflict_size_load(alloc: Allocation, degree: int) -> int:
    return degree * alloc.size


def _order_by_load(
    allocations: tuple[Allocation, ...],
    degrees: list[int],
    load: Callable[[Allocation, int], int],
) -> tuple[Allocation, ...]:
    """Order by `load` over precomputed conflict degrees (largest first, size ties)."""
    paired = sorted(
        zip(allocations, degrees, strict=True),
        key=lambda pair: (load(pair[0], pair[1]), pair[0].size),
        reverse=True,
    )
    return tuple(alloc for alloc, _ in paired)


def order_by_conflict(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Order by conflict degree (most conflicted first)."""
    return _order_by_load(allocations, _sort_degrees(allocations), _conflict_load)


def order_by_conflict_size(
    allocations: tuple[Allocation, ...],
) -> tuple[Allocation, ...]:
    """Order by conflict degree times size (largest first)."""
    return _order_by_load(allocations, _sort_degrees(allocations), _conflict_size_load)


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


def _order_by_input(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    return allocations


_OrderFn = Callable[[tuple[Allocation, ...]], tuple[Allocation, ...]]

# The portfolio's variants in their tie-breaking order: equal peaks go to the
# earlier entry, mirroring the variant tuple this table replaced.
_PORTFOLIO_ORDERS: tuple[tuple[str, _OrderFn], ...] = (
    ("greedy", _order_by_input),
    ("greedy_by_size", order_by_size),
    ("greedy_by_duration", order_by_duration),
    ("greedy_by_area", order_by_area),
    ("greedy_by_conflict", order_by_conflict),
    ("greedy_by_conflict_size", order_by_conflict_size),
    ("greedy_by_start", order_by_start),
)


class GreedyByAllAllocator(GreedyAllocator):
    """Greedy allocator that runs every variant and keeps the best result."""

    def __init__(self, num_threads: int | None = None) -> None:
        ensure_positive(num_threads, "num_threads", allow_none=True)
        self._num_threads = num_threads

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        # One resident placer serves every variant: the conflict adjacency is
        # built once and each order crosses the C++ boundary as a permutation.
        # A workload the placer refuses would have failed all seven variants.
        try:
            placer = FirstFitPlacer(allocations)
        except ValueError as e:
            raise RuntimeError("Every allocator variant failed") from e

        positions = {alloc.id: i for i, alloc in enumerate(allocations)}

        def score(order: _OrderFn) -> tuple[int, list[int]]:
            permutation = [positions[alloc.id] for alloc in order(allocations)]
            return placer.peak(permutation), permutation

        workers = min(resolve_num_threads(self._num_threads), len(_PORTFOLIO_ORDERS))
        scored = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(score, order) for _, order in _PORTFOLIO_ORDERS]
            for (name, _), future in zip(_PORTFOLIO_ORDERS, futures, strict=True):
                try:
                    scored.append(future.result())
                except Exception:
                    logger.warning(
                        "Variant %s failed; skipping it", name, exc_info=True
                    )

        if not scored:
            raise RuntimeError("Every allocator variant failed")
        _, best_permutation = min(scored, key=lambda item: item[0])
        return tuple(placer.place(best_permutation))
