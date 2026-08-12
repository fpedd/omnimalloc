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
from omnimalloc.common.constants import (
    DEFAULT_CLOSURE_CAP,
    DEFAULT_MATERIALIZE_BUDGET,
    DEFAULT_WORK_BUDGET,
)
from omnimalloc.common.validation import ensure_non_negative
from omnimalloc.primitives.allocation import Allocation, IdType
from omnimalloc.primitives.utils import ensure_unique_ids

from ._clock import uniform_dim


def antichain_pressure(
    allocations: Sequence[Allocation],
    work_budget: int | None = DEFAULT_MATERIALIZE_BUDGET,
) -> int:
    """Peak memory pressure: exact max-weight antichain of the happens-before order.

    The tightest order-derived lower bound on any placement's peak, to certify
    optimality rather than run hot. Raises `RuntimeError` past `work_budget`.
    """
    ensure_non_negative(work_budget, "work_budget", allow_none=True)
    return _antichain_pressure(allocations, work_budget)


def closure_pressure(
    allocations: Sequence[Allocation], closure_cap: int | None = DEFAULT_CLOSURE_CAP
) -> int:
    """Lower bound on any placement's peak: max total size jointly live at one cut.

    The looser bound: pairwise-concurrent allocations need not share a cut, so
    this can sit below `antichain_pressure`. Raises past `closure_cap`.
    """
    ensure_non_negative(closure_cap, "closure_cap", allow_none=True)
    return _closure_pressure(allocations, closure_cap)


def placement_pressure(allocations: Sequence[Allocation]) -> int:
    """Peak of a placement: the highest occupied address, max(offset + size).

    An upper bound on `antichain_pressure`, equal to the max entry of
    `placement_pressure_per_allocation`. Raises on unplaced input.
    """
    uniform_dim(allocations)
    heights = []
    for alloc in allocations:
        height = alloc.height
        if height is None:
            raise ValueError("Placement pressure requires placed allocations")
        heights.append(height)
    return max(heights, default=0)


def antichain_pressure_per_allocation(
    allocations: Sequence[Allocation],
    work_budget: int | None = DEFAULT_MATERIALIZE_BUDGET,
) -> dict[IdType, int]:
    """Peak pressure over each allocation's own lifetime, keyed by id.

    `antichain_pressure` restricted to cuts where each allocation is live; the
    max entry equals `antichain_pressure`. Raises past `work_budget`.
    """
    ensure_non_negative(work_budget, "work_budget", allow_none=True)
    ensure_unique_ids(allocations, "allocation")
    peaks = _antichain_pressure_per_allocation(allocations, work_budget)
    return _keyed_by_id(allocations, peaks)


def closure_pressure_per_allocation(
    allocations: Sequence[Allocation], closure_cap: int | None = DEFAULT_CLOSURE_CAP
) -> dict[IdType, int]:
    """Lower bound on the peak while each allocation is live, keyed by id.

    `closure_pressure` restricted to cuts where each allocation is live; the max
    entry equals `closure_pressure`. Raises past `closure_cap`.
    """
    ensure_non_negative(closure_cap, "closure_cap", allow_none=True)
    ensure_unique_ids(allocations, "allocation")
    peaks = _closure_pressure_per_allocation(allocations, closure_cap)
    return _keyed_by_id(allocations, peaks)


def placement_pressure_per_allocation(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> dict[IdType, int]:
    """Placement-certified peak over each allocation's lifetime, keyed by id.

    Read off assigned offsets: the highest occupied address among each allocation
    and its conflict neighbors, whose max entry equals `placement_pressure`.
    """
    ensure_non_negative(work_budget, "work_budget", allow_none=True)
    ensure_unique_ids(allocations, "allocation")
    peaks = _placement_pressure_per_allocation(allocations, work_budget)
    return _keyed_by_id(allocations, peaks)


def _keyed_by_id(
    allocations: Sequence[Allocation], peaks: list[int]
) -> dict[IdType, int]:
    return {alloc.id: peak for alloc, peak in zip(allocations, peaks, strict=True)}
