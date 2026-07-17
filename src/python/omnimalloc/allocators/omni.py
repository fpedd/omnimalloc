#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import omni_place
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.common.deadline import ensure_valid_budget
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class OmniAllocator(BaseAllocator):
    """Generalized C++ greedy-portfolio allocator for scalar and vector time.

    Linearizes vector-clock lifetimes to surrogate scalars when the
    happens-before order allows, otherwise places truthfully on the vector
    conflict graph; either way the best of the seven greedy first-fit orders
    wins (see src/cpp/allocators/omni.cpp). A finite `linearize_budget`
    keeps the linearize attempt from dominating the placement it is meant
    to speed up; pass `None` to always decide linearizability.
    """

    supports_vector_time = True

    def __init__(self, *, linearize_budget: int | None = DEFAULT_WORK_BUDGET) -> None:
        ensure_valid_budget(linearize_budget, name="linearize_budget")
        self._linearize_budget = linearize_budget

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(omni_place(allocations, self._linearize_budget))
