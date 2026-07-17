#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import conflict_degrees as _conflict_degrees
from omnimalloc._cpp import conflicts as _conflicts
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.deadline import ensure_valid_budget
from omnimalloc.primitives.allocation import Allocation, IdType
from omnimalloc.primitives.utils import ensure_unique_ids


def conflicts(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> dict[IdType, set[IdType]]:
    """Conflict map: each allocation's id to the ids it must not share addresses with.

    The happens-before conflict relation every placement packs against.
    Symmetric and total — every allocation is a key, conflict-free ones map
    to an empty set. Handles scalar and vector-clock lifetimes (mutually
    concurrent clocks conflict). C++ sweep (analysis/conflicts.cpp). Raises
    `RuntimeError` once the sweep (quadratic in the worst case)
    exceeds `work_budget`; pass `None` to always compute.
    """
    ensure_valid_budget(work_budget)
    ensure_unique_ids(allocations)
    return _conflicts(allocations, work_budget)


def conflict_degrees(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> list[int]:
    """Conflict count per allocation, aligned with input order.

    The degree sequence of the conflict relation behind `conflicts`,
    without materializing the adjacency. Positional rather than id-keyed,
    so duplicate ids are allowed and counted with multiplicity. Scalar
    lifetimes count in O(N log N) without enumerating pairs; vector clocks
    take the pairwise C++ sweep (quadratic in the worst case), raising
    `RuntimeError` once it exceeds `work_budget` — pass `None` to
    always compute.
    """
    ensure_valid_budget(work_budget)
    return _conflict_degrees(allocations, work_budget)
