#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class NaiveAllocator(BaseAllocator):
    """Naive allocator that places allocations sequentially."""

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        placed_allocations: list[Allocation] = []
        current_offset = 0

        for current_alloc in allocations:
            new_alloc = current_alloc.with_offset(current_offset)
            placed_allocations.append(new_alloc)
            assert new_alloc.offset is not None
            current_offset += new_alloc.size

        return tuple(placed_allocations)
