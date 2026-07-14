#
# SPDX-License-Identifier: Apache-2.0
#

from itertools import pairwise

from .primitives import Allocation
from .primitives.vector_clock import ensure_uniform_dim, happens_before_pairs


def try_linearize(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...] | None:
    """Synthesize scalar lifetimes with the identical conflict relation, or None.

    Succeeds iff the happens-before order is an interval order (equivalently,
    the conflict graph is an interval graph): any 2+2 in the order induces a
    chordless 4-cycle of conflicts, which no intervals can realize. A success
    unlocks scalar-only allocators (minimalloc, supermalloc) for that instance;
    offsets carry over unchanged since the conflict relation — and thus the
    packing problem — is identical.
    """
    if ensure_uniform_dim(allocations) == 1:
        return allocations

    predecessor_sets: list[set[int]] = [set() for _ in allocations]
    successors: list[list[int]] = [[] for _ in allocations]
    for i, j in happens_before_pairs(allocations):
        predecessor_sets[j].add(i)
        successors[i].append(j)
    predecessors = [frozenset(preds) for preds in predecessor_sets]

    # Interval-order test: the strict-predecessor sets must form a chain under
    # inclusion (Fishburn: no induced 2+2).
    chain = sorted(set(predecessors), key=len)
    for smaller, larger in pairwise(chain):
        if not smaller < larger:
            return None

    # Canonical magnitude representation: start = rank of the predecessor set
    # in the chain, end = smallest rank among strict successors' sets. Then
    # end_i <= start_j iff i happens-before j, so conflicts are preserved.
    rank = {predecessor_set: r for r, predecessor_set in enumerate(chain)}
    ends_scalar = [
        min((rank[predecessors[j]] for j in succs), default=len(chain))
        for succs in successors
    ]
    return tuple(
        Allocation(
            id=alloc.id,
            size=alloc.size,
            start=rank[preds],
            end=end,
            offset=alloc.offset,
            kind=alloc.kind,
        )
        for alloc, preds, end in zip(
            allocations, predecessors, ends_scalar, strict=True
        )
    )
