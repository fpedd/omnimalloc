#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import omni_place
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.validation import ensure_non_negative
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class OmniAllocator(BaseAllocator):
    """Generalized C++ greedy-portfolio allocator for scalar and vector time.

    Linearizes vector-clock lifetimes to surrogate scalars when the order allows,
    else places on the vector conflict graph. Invariant under permuting lanes.
    """

    supports_vector_time = True
    supports_pinned = True

    def __init__(self, linearize_budget: int | None = DEFAULT_WORK_BUDGET) -> None:
        ensure_non_negative(linearize_budget, "linearize_budget", allow_none=True)
        self._linearize_budget = linearize_budget

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(omni_place(allocations, self._linearize_budget))
