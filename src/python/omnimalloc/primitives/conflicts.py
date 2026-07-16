#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import compute_temporal_overlaps

from .allocation import Allocation, IdType
from .utils import ensure_unique_ids


def get_conflicts(allocations: tuple[Allocation, ...]) -> dict[IdType, set[IdType]]:
    """Temporal conflict map: each allocation's id to the ids it overlaps in time.

    The relation every placement packs against: conflicting allocations must
    occupy disjoint address ranges. Symmetric and total — every allocation is
    a key, conflict-free ones map to an empty set. Handles scalar and
    vector-clock lifetimes (mutually concurrent clocks conflict). C++ sweep
    (allocators/first_fit.cpp).
    """
    ensure_unique_ids(allocations)
    overlaps = compute_temporal_overlaps(list(allocations))
    return {alloc.id: overlaps.get(alloc.id, set()) for alloc in allocations}
