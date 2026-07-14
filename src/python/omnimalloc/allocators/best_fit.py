#
# SPDX-License-Identifier: Apache-2.0
#

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

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(_BestFitAllocatorCpp().allocate(list(allocations)))
