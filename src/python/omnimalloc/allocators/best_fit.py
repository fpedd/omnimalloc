#
# SPDX-License-Identifier: Apache-2.0
#

from functools import cached_property

from omnimalloc._cpp import BestFitAllocatorCpp as _BestFitAllocatorCpp
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class BestFitAllocator(BaseAllocator):
    """Greedy allocator that places each buffer in the smallest sufficient gap.

    Unlike first-fit (which takes the first gap wide enough among overlapping
    placements), best-fit scans every such gap and picks the tightest one, a
    classic bin-packing strategy that tends to leave larger, more broadly
    useful gaps free for later, bigger allocations.
    """

    @cached_property
    def _cpp_allocator(self) -> _BestFitAllocatorCpp:
        return _BestFitAllocatorCpp()

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(self._cpp_allocator.allocate(list(allocations)))
