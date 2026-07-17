#
# SPDX-License-Identifier: Apache-2.0
#

import logging
import os
import sys
from dataclasses import dataclass
from enum import Enum

from omnimalloc._cpp import (
    Partition,
    SearchOptions,
    Solution,
    greedy_many,
    solve_many,
)
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.common.constants import DEFAULT_TIMEOUT
from omnimalloc.common.deadline import (
    deadline_expired,
    deadline_remaining,
    ensure_valid_timeout,
    make_deadline,
)
from omnimalloc.primitives.allocation import Allocation

logger = logging.getLogger(__name__)


class SortKey(str, Enum):
    """Sort-key characters accepted by the C++ `reorder`."""

    AREA = "A"
    SECTIONS = "C"
    LOWER = "L"
    OVERLAPS = "O"
    SECTION_TOTAL = "T"
    UPPER = "U"
    WIDTH = "W"
    SIZE = "Z"


Heuristic = tuple[SortKey, ...]

DEFAULT_HEURISTICS: tuple[Heuristic, ...] = (
    (SortKey.WIDTH, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.SECTION_TOTAL, SortKey.AREA, SortKey.WIDTH),
    (SortKey.SECTION_TOTAL, SortKey.WIDTH, SortKey.AREA),
)

GREEDY_HEURISTICS: tuple[Heuristic, ...] = (
    (SortKey.AREA, SortKey.WIDTH, SortKey.SECTION_TOTAL),
    (SortKey.AREA, SortKey.SECTION_TOTAL, SortKey.WIDTH),
    (SortKey.WIDTH, SortKey.SECTION_TOTAL, SortKey.AREA),
    (SortKey.SIZE, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.SIZE, SortKey.SECTION_TOTAL, SortKey.AREA),
    (SortKey.OVERLAPS, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.OVERLAPS, SortKey.SECTION_TOTAL, SortKey.AREA),
    (SortKey.LOWER, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.UPPER, SortKey.AREA, SortKey.SECTION_TOTAL),
)


@dataclass(frozen=True)
class SupermallocConfig:
    """Configuration for the SupermallocAllocator."""

    # Wall-clock budget in seconds for greedy + search (problem setup is not
    # counted against it); None lets the search run to optimality.
    timeout: float | None = DEFAULT_TIMEOUT
    heuristics: tuple[Heuristic, ...] = DEFAULT_HEURISTICS
    cores: int | None = None
    canonical: bool = True
    dominance: bool = True
    floor_inference: bool = True
    monotonic_floor: bool = True
    decompose: bool = True

    def __post_init__(self) -> None:
        ensure_valid_timeout(self.timeout)
        if not self.heuristics:
            raise ValueError("SupermallocConfig requires at least one heuristic")

    def num_threads(self) -> int:
        if self.cores is None:
            return os.cpu_count() or 1
        return max(1, self.cores)

    def search_options(self) -> SearchOptions:
        return SearchOptions(
            canonical=self.canonical,
            dominance=self.dominance,
            floor_inference=self.floor_inference,
            monotonic_floor=self.monotonic_floor,
            decompose=self.decompose,
        )


@dataclass(frozen=True)
class _Portfolio:
    """Search invariants for one allocate() run."""

    partitions: list[Partition]
    options: SearchOptions
    threads: int
    # Absolute time.monotonic() deadline; None means the search is unbounded.
    deadline: float | None

    def remaining(self) -> float | None:
        """Seconds left on the budget (0.0 once expired), or None when unbounded."""
        return deadline_remaining(self.deadline)

    def expired(self) -> bool:
        return deadline_expired(self.deadline)

    def solve(self, bounds: tuple[int, ...]) -> Solution | None:
        """Run one portfolio round, or None once the budget has expired.

        The budget is read once per round so the expiry check and the round's
        timeout always agree.
        """
        remaining = self.remaining()
        if remaining is not None and remaining <= 0:
            return None
        members = [p.with_bound(b) for b in bounds for p in self.partitions]
        return solve_many(
            members,
            sys.maxsize,
            remaining,
            max(bounds),
            self.options,
            self.threads,
        )


def _bound_ladder(low: int, high: int, rungs: int) -> tuple[int, ...]:
    """Exclusive search bounds from the incumbent down toward the optimum."""
    gap = high - low
    ladder = [high, low + 1, low + gap // 4, low + gap // 8]
    unique = sorted(set(ladder[:rungs]), reverse=True)
    return tuple(b for b in unique if low < b <= high)


def _search(portfolio: _Portfolio, low: int, height: int) -> Solution | None:
    """Run the concurrent bound-ladder search below the incumbent `height`."""
    best: Solution | None = None
    rungs = max(1, portfolio.threads // len(portfolio.partitions))
    while height > low:
        result = portfolio.solve(_bound_ladder(low, height, rungs))
        if result is None:
            break
        best, height = result, result.height

    if height > low and portfolio.expired():
        logger.debug("Supermalloc timed out above lower bound: %d > %d", height, low)

    return best


class SupermallocAllocator(BaseAllocator):
    """Portfolio branch-and-bound allocator built on a C++ partition solver."""

    # The partition solver's section grid needs a linear timeline
    supports_vector_time = False

    def __init__(self, config: SupermallocConfig | None = None) -> None:
        super().__init__()
        self._config = config or SupermallocConfig()

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        threads = self._config.num_threads()
        base = Partition.from_allocations(allocations)
        heuristics = self._config.heuristics
        heuristic_codes = ["".join(h) for h in heuristics]
        greedy_codes = [*heuristic_codes, ""] + [
            "".join(h) for h in GREEDY_HEURISTICS if h not in heuristics
        ]

        portfolio = _Portfolio(
            partitions=[base.reorder(code) for code in heuristic_codes],
            options=self._config.search_options(),
            threads=threads,
            # Deliberately started after partition construction and the
            # reorders above: the budget covers greedy + search only.
            deadline=make_deadline(self._config.timeout),
        )

        incumbent = greedy_many(base, greedy_codes, portfolio.remaining(), threads)
        best = _search(portfolio, base.lower_bound, incumbent.height)
        if best is None:
            best = incumbent
        return tuple(
            a.with_offset(offset)
            for a, offset in zip(best.allocations, best.offsets, strict=True)
        )
