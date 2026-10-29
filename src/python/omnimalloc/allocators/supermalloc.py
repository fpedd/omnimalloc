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

        Ablations call `_cpp.try_solve_many` directly.
        """
        members = [p.with_bound(b) for b in bounds for p in self.partitions]
        return self._run(members, max(bounds), None, self.threads)

    def solve_member(self, index: int, bound: int, max_nodes: int) -> Solution | None:
        """Search one member alone under a node budget, which makes it repeatable."""
        member = self.partitions[index].with_bound(bound)
        return self._run([member], bound, max_nodes, 1)

    def _run(
        self, members: list[Partition], bound: int, max_nodes: int | None, threads: int
    ) -> Solution | None:
        # The budget is read once so the expiry check and the timeout agree
        remaining = self.remaining()
        if remaining is not None and remaining <= 0:
            return None
        return try_solve_many(
            members,
            bound,
            max_nodes,
            canonical=True,
            dominance=True,
            floor_inference=True,
            monotonic_floor=True,
            decompose=True,
            timeout=remaining,
            num_threads=threads,
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

    Returns the best solution and whether optimality was proved: the solver
    reports "none below" and "budget gone" alike, so time left is the proof.
    """
    best: Solution | None = None
    rungs = max(1, portfolio.threads // len(portfolio.partitions))
    exhausted = False
    while peak > low:
        result = portfolio.solve(_bound_ladder(low, peak, rungs))
        if result is None:
            exhausted = not portfolio.expired()
            break
        best, peak = result, result.peak

    proved_optimal = peak <= low or exhausted
    if not proved_optimal:
        logger.debug("Supermalloc timed out above lower bound: %d > %d", peak, low)

    return best, proved_optimal


def _rederive(portfolio: _Portfolio, peak: int, n: int) -> Solution | None:
    """Place the proved optimum `peak` again, independent of thread timing.

    Concurrent members prune against a shared bound, so which one places the
    optimum, and how, varies from run to run. Take instead the first member
    to reach `peak` alone, under the smallest of a quadrupling series of node
    budgets. Returns None if the budget expires first.
    """
    max_nodes = 2 * n
    while not portfolio.expired():
        for index in range(len(portfolio.partitions)):
            solution = portfolio.solve_member(index, peak + 1, max_nodes)
            if solution is not None:
                return solution
        max_nodes *= 4
    return None


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
        pins = self._ensure_preconditions(allocations)
        if not allocations:
            return SupermallocResult(
                allocations=(), peak=0, lower_bound=0, proved_optimal=True
            )
        result = self._solve(allocations)
        self._ensure_postconditions(allocations, result.allocations, pins)
        return result

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

        portfolio = _Portfolio(
            partitions=[base.reorder(code) for code in heuristic_codes],
            threads=threads,
            deadline=deadline,
        )

        incumbent = greedy_pack_portfolio(
            base, greedy_codes, portfolio.remaining(), threads
        )
        best, proved_optimal = _search(portfolio, base.lower_bound, incumbent.peak)
        # The placement still depends on timing once the budget runs out
        if best is not None and proved_optimal:
            best = _rederive(portfolio, best.peak, len(allocations)) or best
        if best is None:
            best = incumbent
        return SupermallocResult(
            allocations=tuple(best.allocations),
            peak=best.peak,
            lower_bound=base.lower_bound,
            proved_optimal=proved_optimal,
        )
