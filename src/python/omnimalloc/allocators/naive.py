#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.common.intervals import stack_around_pins
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class NaiveAllocator(BaseAllocator):
    """Naive allocator that places allocations sequentially."""

    supports_vector_time = True
    supports_pinned = True

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        offsets = stack_around_pins(
            [alloc.size for alloc in allocations],
            [alloc.offset for alloc in allocations],
        )
        return tuple(
            alloc if alloc.offset is not None else alloc.with_offset(offset)
            for alloc, offset in zip(allocations, offsets, strict=True)
        )
