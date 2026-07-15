#
# SPDX-License-Identifier: Apache-2.0
#

# DEFAULT_WORK_BUDGET caps the implicit pressure query (`Pool.pressure`),
# bounding both the linearize attempt and the antichain flow so it can never
# hang or OOM on huge vector-clock instances; exported from C++ so it cannot
# drift from the OmniAllocator linearize budget.
from omnimalloc._cpp import (
    DEFAULT_WORK_BUDGET,
    antichain_pressure,
    closure_pressure,
    per_allocation_antichain_pressure,
    per_allocation_closure_pressure,
    per_allocation_placement_pressure,
)

from .allocation import Allocation, IdType
from .utils import ensure_unique_ids

DEFAULT_CLOSURE_CAP = 1 << 14


def get_pressure(
    allocations: tuple[Allocation, ...], work_budget: int = DEFAULT_WORK_BUDGET
) -> int:
    """Peak memory pressure (max-weight antichain of the happens-before order).

    Linearizable instances take the O(N log N) sweep — pairwise-overlapping
    intervals share a common time point (Helly), so the peak cut attains the
    max-weight antichain. Otherwise the exact C++ antichain (weighted Dilworth
    via min flow); see `get_antichain_pressure`. Runs under `work_budget` and
    raises rather than hang when the exact flow would exceed it;
    `get_antichain_pressure` is the unbudgeted query.
    """
    return antichain_pressure(list(allocations), work_budget)


def get_antichain_pressure(allocations: tuple[Allocation, ...]) -> int:
    """Exact max-weight antichain of the happens-before order.

    The tightest order-derived lower bound on any placement's peak: pairwise
    conflicts force disjoint address ranges. C++ weighted Dilworth (min flow
    with per-allocation lower bounds); built to certify allocator optimality
    on small and medium instances, not for the 10k+ hot path.
    """
    return antichain_pressure(list(allocations))


def get_closure_pressure(
    allocations: tuple[Allocation, ...], closure_cap: int = DEFAULT_CLOSURE_CAP
) -> int:
    """Exact realizable peak: the max total size jointly live at one cut.

    C++ enumeration of the join-closure of the birth clocks. Pairwise-
    concurrent allocations need not share a cut, so this can sit strictly
    below `get_antichain_pressure`; both soundly lower-bound any placement's
    peak. Raises once the closure exceeds `closure_cap`.
    """
    peak = closure_pressure(list(allocations), closure_cap)
    if peak is None:
        raise RuntimeError(
            f"Join closure exceeded closure_cap={closure_cap}; raise the cap"
        )
    return peak


def get_per_allocation_pressure(
    allocations: tuple[Allocation, ...],
) -> dict[IdType, int]:
    """Peak pressure over each allocation's own lifetime, keyed by id.

    The max-weight antichain through each allocation: the tightest
    order-derived lower bound on the pressure any placement can exhibit
    while that allocation is live; the max entry equals `get_pressure`.
    Linearizable instances take one O(N log N) window sweep, genuinely
    partial orders solve one pinned antichain (min flow over the conflict
    neighborhood) per distinct lifetime — exact, but built to certify
    placements, not for the 10k+ hot path.
    """
    ensure_unique_ids(allocations)
    peaks = per_allocation_antichain_pressure(list(allocations))
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
    per-allocation pressure whose max entry equals the placement's peak.
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
