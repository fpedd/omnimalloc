#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from functools import cache, cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator

from .allocation import Allocation, IdType
from .utils import get_pressure


@dataclass(frozen=True)
class Pool:
    """A collection of allocations sharing a memory region."""

    id: IdType
    allocations: tuple[Allocation, ...]
    offset: int | None = None

    def __post_init__(self) -> None:
        if len({alloc.id for alloc in self.allocations}) != len(self.allocations):
            raise ValueError("allocation ids must be unique")
        if self.offset is not None and self.offset < 0:
            raise ValueError(f"offset must be non-negative, got {self.offset}")

    @cached_property
    def size(self) -> int:
        """Actual memory used (max - min of allocated offsets)."""
        if not self.is_allocated:
            raise ValueError("cannot compute size of unallocated pool")
        offsets = [
            alloc.offset for alloc in self.allocations if alloc.offset is not None
        ]
        ends = [
            alloc.offset + alloc.size
            for alloc in self.allocations
            if alloc.offset is not None
        ]
        return max(ends, default=0) - min(offsets, default=0)

    @cached_property
    def total_size(self) -> int:
        """Sum of all allocation sizes (ignoring temporal overlap)."""
        return sum(alloc.size for alloc in self.allocations)

    @cached_property
    def pressure(self) -> int:
        """Peak memory pressure (max cut through all buffer lifetimes)."""
        return get_pressure(self.allocations)

    @cached_property
    def efficiency(self) -> float:
        """Allocation efficiency: ratio of pressure to allocated size."""
        if not self.is_allocated:
            raise ValueError("cannot compute efficiency of unallocated pool")
        if self.size == 0:
            return 1.0 if self.pressure == 0 else 0.0
        return self.pressure / self.size

    @cached_property
    def is_allocated(self) -> bool:
        """True if all allocations have been assigned memory offsets."""
        return all(alloc.offset is not None for alloc in self.allocations)

    @cache
    def overlaps(self, other: "Pool") -> bool:
        """True if pools overlap in memory space."""
        if self.offset is None or other.offset is None:
            return False
        return (
            self.offset < other.offset + other.size
            and other.offset < self.offset + self.size
        )

    @cache
    def with_allocations(self, allocations: tuple[Allocation, ...]) -> "Pool":
        """Return new Pool with specified allocations."""
        return Pool(id=self.id, offset=self.offset, allocations=allocations)

    def allocate(self, allocator: "BaseAllocator") -> "Pool":
        """Apply allocator to assign memory offsets to all allocations."""
        return self.with_allocations(allocator.allocate(self.allocations))
