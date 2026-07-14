#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class NaiveAllocator(BaseAllocator):
    """Naive allocator that places allocations sequentially."""

    supports_vector_time = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        placed_allocations: list[Allocation] = []
        current_offset = 0

        for current_alloc in allocations:
            placed_allocations.append(current_alloc.with_offset(current_offset))
            current_offset += current_alloc.size

        return tuple(placed_allocations)
