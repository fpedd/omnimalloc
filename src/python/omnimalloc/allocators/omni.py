#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import OmniAllocatorCpp as _OmniAllocatorCpp
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class OmniAllocator(BaseAllocator):
    """Generalized C++ greedy-portfolio allocator for scalar and vector time.

    Linearizes vector-clock lifetimes to surrogate scalars when the
    happens-before order allows, otherwise places truthfully on the vector
    conflict graph; either way the best of the seven greedy first-fit orders
    wins (see src/cpp/allocators/omni.cpp).
    """

    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(_OmniAllocatorCpp().allocate(list(allocations)))
