#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import antichain_pressure as _antichain_pressure
from omnimalloc._cpp import (
    antichain_pressure_per_allocation as _antichain_pressure_per_allocation,
)
from omnimalloc._cpp import closure_pressure as _closure_pressure
from omnimalloc._cpp import (
    closure_pressure_per_allocation as _closure_pressure_per_allocation,
)
from omnimalloc._cpp import (
    placement_pressure_per_allocation as _placement_pressure_per_allocation,
)
from omnimalloc.common.constants import DEFAULT_CLOSURE_CAP, DEFAULT_WORK_BUDGET
from omnimalloc.common.deadline import ensure_valid_budget
from omnimalloc.primitives.allocation import Allocation, IdType
from omnimalloc.primitives.utils import ensure_unique_ids


def antichain_pressure(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> int:
    """Peak memory pressure: exact max-weight antichain of the happens-before order.

    The tightest order-derived lower bound on any placement's peak: pairwise
    conflicts force disjoint address ranges. Linearizable instances take the
    O(N log N) sweep — pairwise-overlapping intervals share a common time
    point (Helly), so the peak cut attains the max-weight antichain. Genuinely
    partial orders solve the exact C++ antichain (weighted Dilworth via min
    flow), built to certify allocator optimality on small and medium
    instances, not for the 10k+ hot path. Raises `RuntimeError` once
    the flow work exceeds `work_budget`; pass `None` to always compute.
    """
    ensure_valid_budget(work_budget)
    return _antichain_pressure(allocations, work_budget)


def closure_pressure(
    allocations: Sequence[Allocation], closure_cap: int | None = DEFAULT_CLOSURE_CAP
) -> int:
    """Exact realizable peak: the max total size jointly live at one cut.

    C++ enumeration of the join-closure of the birth clocks. Pairwise-
    concurrent allocations need not share a cut, so this can sit strictly
    below `antichain_pressure`; both soundly lower-bound any placement's peak. Raises
    `RuntimeError` once the closure exceeds `closure_cap`; pass `None` to
    always enumerate — memory then grows with the closure itself.
    """
    ensure_valid_budget(closure_cap, name="closure_cap")
    return _closure_pressure(allocations, closure_cap)


def placement_pressure(allocations: Sequence[Allocation]) -> int:
    """Peak of a placement: the highest occupied address, max(offset + size).

    Simply the pressure the placement realizes after allocation — an upper
    bound on `antichain_pressure` (and so on `closure_pressure`), equal to the max
    entry of `placement_pressure_per_allocation`. Raises `ValueError` on
    unplaced input.
    """
    heights = []
    for alloc in allocations:
        height = alloc.height
        if height is None:
            raise ValueError("Placement pressure requires placed allocations")
        heights.append(height)
    return max(heights, default=0)


def antichain_pressure_per_allocation(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> dict[IdType, int]:
    """Peak pressure over each allocation's own lifetime, keyed by id.

    The max-weight antichain through each allocation: the tightest
    order-derived lower bound on the pressure any placement can exhibit
    while that allocation is live; the max entry equals `antichain_pressure`.
    Linearizable instances take one O(N log N) window sweep, genuinely
    partial orders solve one pinned antichain (min flow over the conflict
    neighborhood) per distinct lifetime — exact, but built to certify
    placements, not for the 10k+ hot path. Raises `RuntimeError`
    once the linearize attempt or a pinned flow exceeds `work_budget`;
    pass `None` to always compute.
    """
    ensure_valid_budget(work_budget)
    ensure_unique_ids(allocations)
    peaks = _antichain_pressure_per_allocation(allocations, work_budget)
    return _keyed_by_id(allocations, peaks)


def closure_pressure_per_allocation(
    allocations: Sequence[Allocation], closure_cap: int | None = DEFAULT_CLOSURE_CAP
) -> dict[IdType, int]:
    """Exact realizable peak while each allocation is live, keyed by id.

    The max total size at any join-closure cut where the allocation is
    live. Can sit elementwise strictly below `antichain_pressure_per_allocation`,
    since pairwise-concurrent allocations need not share a cut; the max
    entry equals `closure_pressure`. Raises `RuntimeError` once the
    closure exceeds `closure_cap`; pass `None` to always enumerate —
    memory then grows with the closure itself.
    """
    ensure_valid_budget(closure_cap, name="closure_cap")
    ensure_unique_ids(allocations)
    peaks = _closure_pressure_per_allocation(allocations, closure_cap)
    return _keyed_by_id(allocations, peaks)


def placement_pressure_per_allocation(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> dict[IdType, int]:
    """Placement-certified peak over each allocation's lifetime, keyed by id.

    Read off assigned offsets: the highest occupied address among each
    allocation and its conflict neighbors, an upper bound on every exact
    per-allocation pressure whose max entry equals `placement_pressure`.
    Raises `ValueError` on unplaced input and `RuntimeError` once the
    vector-clock conflict sweep exceeds `work_budget` (which also bounds
    the internal linearize attempt); pass `None` to always compute.
    """
    ensure_valid_budget(work_budget)
    ensure_unique_ids(allocations)
    peaks = _placement_pressure_per_allocation(allocations, work_budget)
    return _keyed_by_id(allocations, peaks)


# The antichain bound is the canonical/default pressure metric
pressure = antichain_pressure
pressure_per_allocation = antichain_pressure_per_allocation


def _keyed_by_id(
    allocations: Sequence[Allocation], peaks: list[int]
) -> dict[IdType, int]:
    return {alloc.id: peak for alloc, peak in zip(allocations, peaks, strict=True)}
