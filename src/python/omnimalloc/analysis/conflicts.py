#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import compute_conflict_degrees, compute_temporal_overlaps
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.deadline import ensure_valid_budget
from omnimalloc.primitives.allocation import Allocation, IdType
from omnimalloc.primitives.utils import ensure_unique_ids


def get_conflicts(
    allocations: tuple[Allocation, ...], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> dict[IdType, set[IdType]] | None:
    """Temporal conflict map: each allocation's id to the ids it overlaps in time.

    The relation every placement packs against: conflicting allocations must
    occupy disjoint address ranges. Symmetric and total — every allocation is
    a key, conflict-free ones map to an empty set. Handles scalar and
    vector-clock lifetimes (mutually concurrent clocks conflict). C++ sweep
    (analysis/conflicts.cpp). A finite `work_budget` bounds the sweep
    (quadratic in the worst case), giving up (None) instead of stalling;
    pass `None` to always compute the relation.
    """
    ensure_valid_budget(work_budget)
    ensure_unique_ids(allocations)
    overlaps = compute_temporal_overlaps(list(allocations), work_budget)
    if overlaps is None:
        return None
    return {alloc.id: overlaps.get(alloc.id, set()) for alloc in allocations}


def get_conflict_degrees(
    allocations: tuple[Allocation, ...], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> list[int] | None:
    """Temporal conflict count per allocation, aligned with input order.

    The degree sequence of the conflict relation behind `get_conflicts`,
    from the same C++ sweep without materializing the adjacency. Positional
    rather than id-keyed, so duplicate ids are allowed and counted with
    multiplicity. A finite `work_budget` bounds the pairwise sweep
    (quadratic in the worst case), giving up (None) instead of stalling;
    pass `None` to always count.
    """
    ensure_valid_budget(work_budget)
    return compute_conflict_degrees(list(allocations), work_budget)
