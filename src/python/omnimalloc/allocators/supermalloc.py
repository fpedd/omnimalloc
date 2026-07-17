#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from dataclasses import dataclass
from enum import Enum

from omnimalloc._cpp import Partition, Solution, greedy_pack_portfolio, try_solve_many
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.common.constants import DEFAULT_TIMEOUT
from omnimalloc.common.deadline import (
    deadline_expired,
    deadline_remaining,
    ensure_valid_timeout,
    make_deadline,
)
from omnimalloc.common.parallel import ensure_valid_num_threads, resolve_num_threads
from omnimalloc.primitives.allocation import Allocation

logger = logging.getLogger(__name__)


class SortKey(str, Enum):
    """Sort-key characters accepted by the C++ `reorder`."""

    AREA = "A"
    SECTIONS = "C"
    START = "L"
    CONFLICTS = "O"
    SECTION_TOTAL = "T"
    END = "U"
    DURATION = "W"
    SIZE = "Z"


Heuristic = tuple[SortKey, ...]

DEFAULT_HEURISTICS: tuple[Heuristic, ...] = (
    (SortKey.DURATION, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.SECTION_TOTAL, SortKey.AREA, SortKey.DURATION),
    (SortKey.SECTION_TOTAL, SortKey.DURATION, SortKey.AREA),
)

GREEDY_HEURISTICS: tuple[Heuristic, ...] = (
    (SortKey.AREA, SortKey.DURATION, SortKey.SECTION_TOTAL),
    (SortKey.AREA, SortKey.SECTION_TOTAL, SortKey.DURATION),
    (SortKey.DURATION, SortKey.SECTION_TOTAL, SortKey.AREA),
    (SortKey.SIZE, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.SIZE, SortKey.SECTION_TOTAL, SortKey.AREA),
    (SortKey.CONFLICTS, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.CONFLICTS, SortKey.SECTION_TOTAL, SortKey.AREA),
    (SortKey.START, SortKey.AREA, SortKey.SECTION_TOTAL),
    (SortKey.END, SortKey.AREA, SortKey.SECTION_TOTAL),
)


@dataclass(frozen=True)
class _Portfolio:
    """Search invariants for one allocate() run."""

    partitions: list[Partition]
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
        timeout always agree. The five search switches are developer flags
        that stay enabled here; ablations call `_cpp.try_solve_many` directly.
        """
        remaining = self.remaining()
        if remaining is not None and remaining <= 0:
            return None
        members = [p.with_bound(b) for b in bounds for p in self.partitions]
        return try_solve_many(
            members,
            max(bounds),
            None,
            canonical=True,
            dominance=True,
            floor_inference=True,
            monotonic_floor=True,
            decompose=True,
            timeout=remaining,
            num_threads=self.threads,
        )


def _bound_ladder(low: int, high: int, rungs: int) -> tuple[int, ...]:
    """Exclusive search bounds from the incumbent down toward the optimum."""
    gap = high - low
    ladder = [high, low + 1, low + gap // 4, low + gap // 8]
    unique = sorted(set(ladder[:rungs]), reverse=True)
    return tuple(b for b in unique if low < b <= high)


def _search(portfolio: _Portfolio, low: int, peak: int) -> Solution | None:
    """Run the concurrent bound-ladder search below the incumbent `peak`."""
    best: Solution | None = None
    rungs = max(1, portfolio.threads // len(portfolio.partitions))
    while peak > low:
        result = portfolio.solve(_bound_ladder(low, peak, rungs))
        if result is None:
            break
        best, peak = result, result.peak

    if peak > low and portfolio.expired():
        logger.debug("Supermalloc timed out above lower bound: %d > %d", peak, low)

    return best


class SupermallocAllocator(BaseAllocator):
    """Portfolio branch-and-bound allocator built on a C++ partition solver.

    `timeout` (default 3s) is the wall-clock budget for greedy + search
    (problem setup is not counted against it); `None` lets the search run to
    optimality. `num_threads=None` uses all cores.
    """

    # The partition solver's section grid needs a linear timeline
    supports_vector_time = False

    def __init__(
        self,
        *,
        timeout: float | None = DEFAULT_TIMEOUT,
        heuristics: tuple[Heuristic, ...] = DEFAULT_HEURISTICS,
        num_threads: int | None = None,
    ) -> None:
        ensure_valid_timeout(timeout)
        ensure_valid_num_threads(num_threads)
        if not heuristics:
            raise ValueError("SupermallocAllocator requires at least one heuristic")
        self._timeout = timeout
        self._heuristics = heuristics
        self._num_threads = num_threads

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        threads = resolve_num_threads(self._num_threads)
        base = Partition.from_allocations(allocations)
        heuristic_codes = ["".join(h) for h in self._heuristics]
        greedy_codes = [*heuristic_codes, ""] + [
            "".join(h) for h in GREEDY_HEURISTICS if h not in self._heuristics
        ]

        portfolio = _Portfolio(
            partitions=[base.reorder(code) for code in heuristic_codes],
            threads=threads,
            # Deliberately started after partition construction and the
            # reorders above: the budget covers greedy + search only.
            deadline=make_deadline(self._timeout),
        )

        incumbent = greedy_pack_portfolio(
            base, greedy_codes, portfolio.remaining(), threads
        )
        best = _search(portfolio, base.lower_bound, incumbent.peak)
        if best is None:
            best = incumbent
        return tuple(best.allocations)
