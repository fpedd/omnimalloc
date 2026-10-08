#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, cast

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
        # __post_init__ coerces and checks the sequence
        return cls(id=0, allocations=cast("tuple[Allocation, ...]", allocations))

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
        # Allocation sizes are positive, so only an empty pool has size 0
        return self.pressure / self.size if self.size else 1.0

    @cached_property
    def is_allocated(self) -> bool:
        """True if all allocations have been assigned memory offsets."""
        return all(alloc.offset is not None for alloc in self.allocations)

    @cached_property
    def any_allocated(self) -> bool:
        """True if any allocation has been assigned a memory offset."""
        return any(alloc.offset is not None for alloc in self.allocations)

    def with_allocations(self, allocations: tuple[Allocation, ...]) -> "Pool":
        """Return new Pool with specified allocations."""
        return Pool(id=self.id, offset=self.offset, allocations=allocations)

    def allocate(self, allocator: "BaseAllocator") -> "Pool":
        """Assign offsets to the unpinned allocations, preserving input order."""
        return self.with_allocations(allocator.allocate(self.allocations))
