#
# SPDX-License-Identifier: Apache-2.0
#

# DEFAULT_WORK_BUDGET and DEFAULT_CLOSURE_CAP bound the exact queries so
# implicit callers (`Pool.pressure`) can never hang or OOM on huge
# vector-clock instances; both are exported from C++, next to the algorithms
# they tune, so they cannot drift from the OmniAllocator linearize budget.
from omnimalloc._cpp import (
    DEFAULT_CLOSURE_CAP,
    DEFAULT_WORK_BUDGET,
    antichain_pressure,
    closure_pressure,
    per_allocation_antichain_pressure,
    per_allocation_closure_pressure,
    per_allocation_placement_pressure,
)

from .allocation import Allocation, IdType
from .utils import ensure_unique_ids


def get_pressure(
    allocations: tuple[Allocation, ...], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> int:
    """Peak memory pressure: exact max-weight antichain of the happens-before order.

    The tightest order-derived lower bound on any placement's peak: pairwise
    conflicts force disjoint address ranges. Linearizable instances take the
    O(N log N) sweep — pairwise-overlapping intervals share a common time
    point (Helly), so the peak cut attains the max-weight antichain. Genuinely
    partial orders solve the exact C++ antichain (weighted Dilworth via min
    flow), built to certify allocator optimality on small and medium
    instances, not for the 10k+ hot path. A finite `work_budget` bounds both
    phases and raises rather than hang when the flow would exceed it; pass
    `None` to always compute the exact answer.
    """
    if work_budget is None:
        return antichain_pressure(list(allocations))
    return antichain_pressure(list(allocations), work_budget)


def get_closure_pressure(
    allocations: tuple[Allocation, ...], closure_cap: int = DEFAULT_CLOSURE_CAP
) -> int:
    """Exact realizable peak: the max total size jointly live at one cut.

    C++ enumeration of the join-closure of the birth clocks. Pairwise-
    concurrent allocations need not share a cut, so this can sit strictly
    below `get_pressure`; both soundly lower-bound any placement's peak.
    Raises once the closure exceeds `closure_cap`.
    """
    peak = closure_pressure(list(allocations), closure_cap)
    if peak is None:
        raise RuntimeError(
            f"Join closure exceeded closure_cap={closure_cap}; raise the cap"
        )
    return peak


def get_placement_pressure(allocations: tuple[Allocation, ...]) -> int:
    """Peak of a placement: the highest occupied address, max(offset + size).

    Simply the pressure the placement realizes after allocation — an upper
    bound on `get_pressure` (and so on `get_closure_pressure`), equal to the
    max entry of `get_per_allocation_placement_pressure`. Requires placed
    allocations.
    """
    heights = []
    for alloc in allocations:
        height = alloc.height
        if height is None:
            raise ValueError("Placement pressure requires placed allocations")
        heights.append(height)
    return max(heights, default=0)


def get_per_allocation_pressure(
    allocations: tuple[Allocation, ...], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> dict[IdType, int]:
    """Peak pressure over each allocation's own lifetime, keyed by id.

    The max-weight antichain through each allocation: the tightest
    order-derived lower bound on the pressure any placement can exhibit
    while that allocation is live; the max entry equals `get_pressure`.
    Linearizable instances take one O(N log N) window sweep, genuinely
    partial orders solve one pinned antichain (min flow over the conflict
    neighborhood) per distinct lifetime — exact, but built to certify
    placements, not for the 10k+ hot path. A finite `work_budget` bounds
    the linearize attempt and each pinned flow and raises rather than hang;
    pass `None` to always compute the exact answer.
    """
    ensure_unique_ids(allocations)
    if work_budget is None:
        peaks = per_allocation_antichain_pressure(list(allocations))
    else:
        peaks = per_allocation_antichain_pressure(list(allocations), work_budget)
    return _keyed_by_id(allocations, peaks)


def get_per_allocation_closure_pressure(
    allocations: tuple[Allocation, ...], closure_cap: int = DEFAULT_CLOSURE_CAP
) -> dict[IdType, int]:
    """Exact realizable peak while each allocation is live, keyed by id.

    The max total size at any join-closure cut where the allocation is
    live. Can sit elementwise strictly below `get_per_allocation_pressure`,
    since pairwise-concurrent allocations need not share a cut; the max
    entry equals `get_closure_pressure`. Raises once the closure exceeds
    `closure_cap`.
    """
    ensure_unique_ids(allocations)
    peaks = per_allocation_closure_pressure(list(allocations), closure_cap)
    if peaks is None:
        raise RuntimeError(
            f"Join closure exceeded closure_cap={closure_cap}; raise the cap"
        )
    return _keyed_by_id(allocations, peaks)


def get_per_allocation_placement_pressure(
    allocations: tuple[Allocation, ...], clique_cap: bool = False
) -> dict[IdType, int]:
    """Placement-certified peak over each allocation's lifetime, keyed by id.

    Read off assigned offsets: the highest occupied address among each
    allocation and its conflict neighbors, an upper bound on every exact
    per-allocation pressure whose max entry equals `get_placement_pressure`.
    With `clique_cap`, entries are additionally capped by their conflict
    clique's total size — elementwise tighter, but the max-equals-peak
    identity no longer holds. Requires placed allocations.
    """
    ensure_unique_ids(allocations)
    peaks = per_allocation_placement_pressure(list(allocations), clique_cap)
    return _keyed_by_id(allocations, peaks)


def _keyed_by_id(
    allocations: tuple[Allocation, ...], peaks: list[int]
) -> dict[IdType, int]:
    return {alloc.id: peak for alloc, peak in zip(allocations, peaks, strict=True)}
