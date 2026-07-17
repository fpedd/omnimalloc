#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

from omnimalloc.common.validation import ensure_non_negative

from .allocation import IdType
from .pool import Pool

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator


@dataclass(frozen=True)
class Memory:
    """A physical memory unit containing one or more pools.

    `capacity` is the declared memory limit (an input); the computed extent
    of a placement is `used_size`. `capacity=None` means unbounded.
    """

    id: IdType
    pools: tuple[Pool, ...]
    capacity: int | None = None

    def __post_init__(self) -> None:
        if len({pool.id for pool in self.pools}) != len(self.pools):
            raise ValueError("pool ids must be unique")
        if self.capacity is not None:
            ensure_non_negative(self.capacity, "capacity")

    @cached_property
    def used_size(self) -> int:
        """Total memory used by all pools."""
        return sum(pool.size for pool in self.pools)

    @cached_property
    def is_allocated(self) -> bool:
        """True if all pools have been allocated."""
        return all(pool.is_allocated for pool in self.pools)

    def with_pools(self, pools: tuple[Pool, ...]) -> "Memory":
        """Return new Memory with specified pools."""
        return Memory(id=self.id, capacity=self.capacity, pools=pools)

    def allocate(self, allocator: "BaseAllocator") -> "Memory":
        """Apply allocator to all pools."""
        return self.with_pools(tuple(p.allocate(allocator) for p in self.pools))
