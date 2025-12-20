#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from functools import cache, cached_property
from typing import TYPE_CHECKING

from .allocation import IdType
from .pool import Pool

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator


@dataclass(frozen=True)
class Memory:
    """A physical memory unit containing one or more pools."""

    id: IdType
    pools: tuple[Pool, ...]
    size: int | None = None

    def __post_init__(self) -> None:
        if len({pool.id for pool in self.pools}) != len(self.pools):
            raise ValueError("pool ids must be unique")
        if self.size is not None and self.size < 0:
            raise ValueError(f"size must be non-negative, got {self.size}")

    @cached_property
    def used_size(self) -> int:
        """Total memory used by all pools."""
        return sum(pool.size for pool in self.pools)

    @cached_property
    def free_size(self) -> int | None:
        """Available memory remaining (None if memory size is unbounded)."""
        if self.size is None:
            return None
        return self.size - self.used_size

    @cached_property
    def utilization(self) -> float | None:
        """Fraction of memory used (None if memory size is unbounded)."""
        if self.size is None:
            return None
        return self.used_size / self.size if self.size > 0 else 0.0

    @cached_property
    def is_allocated(self) -> bool:
        """True if all pools have been allocated."""
        return all(pool.is_allocated for pool in self.pools)

    @cache
    def with_pools(self, pools: tuple[Pool, ...]) -> "Memory":
        """Return new Memory with specified pools."""
        return Memory(id=self.id, size=self.size, pools=pools)

    def allocate(self, allocator: "BaseAllocator") -> "Memory":
        """Apply allocator to all pools."""
        return self.with_pools(tuple(p.allocate(allocator) for p in self.pools))
