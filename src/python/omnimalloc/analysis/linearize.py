#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence

from omnimalloc._cpp import try_linearize as _try_linearize
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.deadline import ensure_valid_budget
from omnimalloc.primitives.allocation import Allocation

from .clock import uniform_dim


def try_linearize(
    allocations: Sequence[Allocation], work_budget: int | None = DEFAULT_WORK_BUDGET
) -> tuple[Allocation, ...] | None:
    """Synthesize scalar lifetimes with the identical conflict relation, or None.

    Within the work budget, succeeds iff the happens-before order is an
    interval order (equivalently, the conflict graph is an interval graph):
    any 2+2 in the order induces a chordless 4-cycle of conflicts, which no
    intervals can realize. A success unlocks scalar-only allocators
    (minimalloc, supermalloc) for that instance; offsets carry over unchanged
    since the conflict relation — and thus the packing problem — is
    identical. Implemented in C++ (analysis/linearize.cpp). `None` means no
    linearization was obtained: the order is not an interval order, or
    deciding would exceed `work_budget`; pass `None` to always decide.
    """
    ensure_valid_budget(work_budget)
    if uniform_dim(allocations) == 1:
        return tuple(allocations)
    linearized = _try_linearize(allocations, work_budget)
    return None if linearized is None else tuple(linearized)
