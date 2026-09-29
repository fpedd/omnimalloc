#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from dataclasses import dataclass, replace
from enum import Enum

from omnimalloc._cpp import (
    Partition,
    SearchResult,
    Solution,
    greedy_pack_portfolio,
    try_solve_many,
)
from omnimalloc.allocators.base import BaseAllocator
from omnimalloc.common.constants import DEFAULT_TIMEOUT
from omnimalloc.common.deadline import (
    deadline_expired,
    deadline_remaining,
    ensure_valid_timeout,
    make_deadline,
)
from omnimalloc.common.parallel import resolve_num_threads
from omnimalloc.common.validation import ensure_positive
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


# Node budget of a member's first round, doubled whenever a round ends with
# neither a solution nor a proof
_FIRST_MAX_NODES = 10_000


@dataclass(frozen=True)
class _Portfolio:
    """Search invariants for one allocate() run."""

    partitions: list[Partition]
    threads: int
    # Absolute time.monotonic() deadline; None means the search is unbounded.
    deadline: float | None

    def expired(self) -> bool:
        return deadline_expired(self.deadline)

    def solve(self, bounds: tuple[int, ...], max_nodes: int | None) -> SearchResult:
        """Run one portfolio round over every (bound, partition) pair.

        Ablations call `_cpp.try_solve_many` directly.
        """
        return try_solve_many(
            self.partitions,
            bounds,
            max_nodes,
            canonical=True,
            dominance=True,
            floor_inference=True,
            monotonic_floor=True,
            decompose=True,
            timeout=deadline_remaining(self.deadline),
            num_threads=self.threads,
        )


def _bound_ladder(low: int, high: int, rungs: int) -> tuple[int, ...]:
    """Exclusive search bounds from the incumbent down toward the optimum."""
    gap = high - low
    ladder = [high, low + 1, low + gap // 4, low + gap // 8]
    unique = sorted(set(ladder[:rungs]), reverse=True)
    return tuple(b for b in unique if low < b <= high)


@dataclass(frozen=True)
class SupermallocResult:
    """A placement plus the search's own verdict on it."""

    allocations: tuple[Allocation, ...]
    peak: int
    lower_bound: int
    proved_optimal: bool


def _search(portfolio: _Portfolio, low: int, peak: int) -> tuple[Solution | None, bool]:
    """Run the concurrent bound-ladder search below the incumbent `peak`.

    Returns the best solution and whether optimality was proved.
    """
    best: Solution | None = None
    # Always the incumbent and low + 1 rungs, however few the threads
    rungs = max(2, portfolio.threads // len(portfolio.partitions))
    # Members beyond the thread count run one after another, and an unbounded
    # one would spend the whole deadline; node budgets let every member run
    serial = rungs * len(portfolio.partitions) > portfolio.threads
    max_nodes = _FIRST_MAX_NODES if serial else None
    while peak > low and not portfolio.expired():
        result = portfolio.solve(_bound_ladder(low, peak, rungs), max_nodes)
        if result.solution is not None:
            best, peak = result.solution, result.solution.peak
        elif result.exhausted:
            return best, True  # no member found anything below `peak`
        elif max_nodes is not None:
            max_nodes *= 2

    if peak > low:
        logger.debug("Supermalloc timed out above lower bound: %d > %d", peak, low)
    return best, peak <= low


class SupermallocAllocator(BaseAllocator):
    """Portfolio branch-and-bound allocator built on a C++ partition solver.

    `timeout` (default 3s) budgets the whole call, but building the partition
    and packing the first heuristic cannot be interrupted, a floor of seconds.
    """

    # The partition solver's section grid needs a linear timeline
    supports_vector_time = False

    def __init__(
        self,
        timeout: float | None = DEFAULT_TIMEOUT,
        heuristics: tuple[Heuristic, ...] = DEFAULT_HEURISTICS,
        num_threads: int | None = None,
    ) -> None:
        ensure_valid_timeout(timeout)
        ensure_positive(num_threads, "num_threads", allow_none=True)
        if not heuristics:
            raise ValueError("SupermallocAllocator requires at least one heuristic")
        self._timeout = timeout
        self._heuristics = heuristics
        self._num_threads = num_threads

    def solve(self, allocations: tuple[Allocation, ...]) -> SupermallocResult:
        """Place the allocations and keep the search's verdict on the result.

        `allocate` returns the placement alone, which cannot say whether the
        search proved optimality or merely ran out of budget.
        """
        self._ensure_preconditions(allocations)
        if not allocations:
            return SupermallocResult(
                allocations=(), peak=0, lower_bound=0, proved_optimal=True
            )
        result = self._solve(allocations)
        return replace(
            result, allocations=self._finish(allocations, result.allocations)
        )

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return self._solve(allocations).allocations

    def _solve(self, allocations: tuple[Allocation, ...]) -> SupermallocResult:
        # Started before problem setup: on a large instance the partition and
        # its reorders are themselves seconds of work, and a caller's budget
        # is the wall clock it is willing to spend, not the search's share
        deadline = make_deadline(self._timeout)
        threads = resolve_num_threads(self._num_threads)
        base = Partition.from_allocations(allocations)
        heuristic_codes = ["".join(h) for h in self._heuristics]
        greedy_codes = [*heuristic_codes, ""] + [
            "".join(h) for h in GREEDY_HEURISTICS if h not in self._heuristics
        ]

        incumbent = greedy_pack_portfolio(
            base, greedy_codes, deadline_remaining(deadline), threads
        )
        best, proved_optimal = None, incumbent.peak <= base.lower_bound
        # Each search member is a reorder, seconds of work at scale, so build
        # them only when the incumbent leaves a gap to close
        if not proved_optimal:
            portfolio = _Portfolio(
                partitions=[base.reorder(code) for code in heuristic_codes],
                threads=threads,
                deadline=deadline,
            )
            best, proved_optimal = _search(portfolio, base.lower_bound, incumbent.peak)
        if best is None:
            best = incumbent
        return SupermallocResult(
            allocations=tuple(best.allocations),
            peak=best.peak,
            lower_bound=base.lower_bound,
            proved_optimal=proved_optimal,
        )
