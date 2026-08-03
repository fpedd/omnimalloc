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

    Succeeds iff the happens-before order is an interval order, unlocking the
    scalar-only allocators. Exceeding `work_budget` raises rather than None.
    """
    ensure_valid_budget(work_budget)
    if uniform_dim(allocations) == 1:
        return tuple(allocations)
    linearized = _try_linearize(allocations, work_budget)
    return None if linearized is None else tuple(linearized)
