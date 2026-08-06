#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import ConflictGraph
from omnimalloc._cpp import conflict_degrees as _conflict_degrees
from omnimalloc.common.constants import (
    DEFAULT_CONFLICT_MAP_BUDGET,
    DEFAULT_WORK_BUDGET,
)
from omnimalloc.common.validation import ensure_non_negative
from omnimalloc.primitives.allocation import Allocation, IdType
from omnimalloc.primitives.utils import ensure_unique_ids


def conflicts(
    allocations: Sequence[Allocation],
    work_budget: int | None = DEFAULT_CONFLICT_MAP_BUDGET,
) -> dict[IdType, set[IdType]]:
    """Conflict map: each allocation's id to the ids it must not share addresses with.

    The happens-before conflict relation every placement packs against. Raises
    past `work_budget`, whose tight default reflects the map, not the sweep.
    """
    ensure_unique_ids(allocations, "allocation")
    graph = conflict_graph(allocations, work_budget)
    ids = [alloc.id for alloc in allocations]
    return {
        ids[row]: {ids[neighbor] for neighbor in graph.neighbors(row)}
        for row in range(len(graph))
    }


def conflict_graph(
    allocations: Sequence[Allocation],
    work_budget: int | None = DEFAULT_WORK_BUDGET,
    max_entries: int | None = None,
) -> ConflictGraph:
    """The relation `conflicts` returns, streamed instead of materialized.

    Keeps the adjacency in C++ CSR form and hands out one positional row at a
    time. `max_entries` caps the CSR, `work_budget` the sweep, neither the other.
    """
    ensure_non_negative(work_budget, "work_budget", allow_none=True)
    ensure_non_negative(max_entries, "max_entries", allow_none=True)
    return ConflictGraph(allocations, work_budget, max_entries)


def conflict_degrees(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> list[int]:
    """Conflict count per allocation, aligned with input order.

    The degree sequence behind `conflicts` without materializing it, positional,
    so duplicate ids count with multiplicity. Scalar lifetimes cost O(N log N).
    """
    ensure_non_negative(work_budget, "work_budget", allow_none=True)
    return _conflict_degrees(allocations, work_budget)
