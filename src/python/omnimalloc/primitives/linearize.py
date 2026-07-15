#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import try_linearize as _try_linearize

from .allocation import Allocation
from .vector_clock import ensure_uniform_dim


def try_linearize(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...] | None:
    """Synthesize scalar lifetimes with the identical conflict relation, or None.

    Succeeds iff the happens-before order is an interval order (equivalently,
    the conflict graph is an interval graph): any 2+2 in the order induces a
    chordless 4-cycle of conflicts, which no intervals can realize. A success
    unlocks scalar-only allocators (minimalloc, supermalloc) for that instance;
    offsets carry over unchanged since the conflict relation — and thus the
    packing problem — is identical. Implemented in C++ (primitives/linearize.cpp).
    """
    if ensure_uniform_dim(allocations) == 1:
        return allocations
    linearized = _try_linearize(list(allocations))
    return None if linearized is None else tuple(linearized)
