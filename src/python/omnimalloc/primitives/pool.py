#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator

from omnimalloc.analysis._pressure import antichain_pressure as _pressure
from omnimalloc.common.validation import ensure_non_negative

from .allocation import Allocation, IdType
from .utils import ensure_allocations, ensure_unique_ids


@dataclass(frozen=True)
class Pool:
    """A collection of allocations sharing a memory region."""

    id: IdType
    allocations: tuple[Allocation, ...]
    offset: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "allocations", ensure_allocations(self.allocations))
        ensure_unique_ids(self.allocations, "allocation")
        if self.offset is not None:
            ensure_non_negative(self.offset, "offset")

    @classmethod
    def from_allocations(cls, allocations: Sequence[Allocation]) -> "Pool":
        """Wrap a raw sequence of allocations in an anonymous pool."""
        return cls(id=0, allocations=ensure_allocations(allocations))

    @cached_property
    def size(self) -> int:
        """Extent from the pool base (offset 0) to the highest allocated end."""
        if not self.is_allocated:
            raise ValueError("cannot compute size of unallocated pool")
        return max((alloc.height or 0 for alloc in self.allocations), default=0)

    @cached_property
    def pressure(self) -> int:
        """Peak memory pressure (max cut through all buffer lifetimes)."""
        return _pressure(self.allocations)

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

    @cached_property
    def any_allocated(self) -> bool:
        """True if any allocation has been assigned a memory offset."""
        return any(alloc.offset is not None for alloc in self.allocations)

    def overlaps(self, other: "Pool") -> bool:
        """True if pools overlap in memory space."""
        if self.offset is None or other.offset is None:
            return False
        return (
            self.offset < other.offset + other.size
            and other.offset < self.offset + self.size
        )

    def with_allocations(self, allocations: tuple[Allocation, ...]) -> "Pool":
        """Return new Pool with specified allocations."""
        return Pool(id=self.id, offset=self.offset, allocations=allocations)

    def allocate(self, allocator: "BaseAllocator") -> "Pool":
        """Assign offsets to the unpinned allocations, preserving input order."""
        allocated = allocator.allocate(self.allocations)
        # Allocators may reorder internally; restore the pool's input order so
        # positions in the returned tuple keep corresponding to the request
        placed_by_id = {a.id: a for a in allocated}
        return self.with_allocations(
            tuple(placed_by_id[a.id] for a in self.allocations)
        )
