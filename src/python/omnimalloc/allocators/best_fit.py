#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import best_fit_place
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class BestFitAllocator(BaseAllocator):
    """Greedy allocator that places each buffer in the smallest sufficient gap.

    Where first-fit takes the first gap wide enough, best-fit scans every gap
    and picks the tightest, leaving larger ones free for later allocations.
    """

    supports_vector_time = True
    supports_pinned = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(best_fit_place(allocations))
