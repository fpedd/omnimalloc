#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass, replace
from functools import cached_property
from itertools import pairwise
from typing import TYPE_CHECKING

from omnimalloc.common.intervals import stack_around_pins
from omnimalloc.common.validation import ensure_non_negative

from .allocation import IdType
from .pool import Pool
from .utils import ensure_items, ensure_unique_ids

if TYPE_CHECKING:
    from omnimalloc.allocators import BaseAllocator


@dataclass(frozen=True)
class Memory:
    """A physical memory unit containing one or more pools."""

    id: IdType
    pools: tuple[Pool, ...]
    size: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "pools", ensure_items(self.pools, Pool, "pools"))
        ensure_unique_ids(self.pools, "pool")
        if self.size is not None:
            ensure_non_negative(self.size, "size")

    @cached_property
    def used_size(self) -> int:
        """Sum of the pools' derived sizes, blind to where pools sit."""
        return sum(pool.size for pool in self.pools)

    @cached_property
    def extent(self) -> int:
        """Highest address any pool occupies: the capacity this memory needs."""
        tops = [p.offset + p.size for p in self.pools if p.offset is not None]
        if len(tops) != len(self.pools):
            raise ValueError("cannot compute extent while pools are unplaced")
        return max(tops, default=0)

    @cached_property
    def is_allocated(self) -> bool:
        """True if all pools have been allocated."""
        return all(pool.is_allocated for pool in self.pools)

    @cached_property
    def any_allocated(self) -> bool:
        """True if any pool has a placed allocation."""
        return any(pool.any_allocated for pool in self.pools)

    def with_pools(self, pools: tuple[Pool, ...]) -> "Memory":
        """Return new Memory with specified pools."""
        return Memory(id=self.id, size=self.size, pools=pools)

    def allocate(self, allocator: "BaseAllocator") -> "Memory":
        """Apply allocator to all pools, then give every unplaced pool a base."""
        return self.with_pools(
            _place_pools(tuple(p.allocate(allocator) for p in self.pools))
        )


def _place_pools(pools: tuple[Pool, ...]) -> tuple[Pool, ...]:
    """Stack the pools that carry no offset around the ones that do."""
    _ensure_pinned_bases_disjoint(pools)
    offsets = stack_around_pins(
        [pool.size for pool in pools], [pool.offset for pool in pools]
    )
    return tuple(
        pool if pool.offset is not None else replace(pool, offset=offset)
        for pool, offset in zip(pools, offsets, strict=True)
    )


def _ensure_pinned_bases_disjoint(pools: tuple[Pool, ...]) -> None:
    """Reject pinned bases that already overlap; no layout can satisfy them."""
    pinned = sorted(
        [(pool.offset, pool) for pool in pools if pool.offset is not None],
        key=lambda item: item[0],
    )
    for (lower_base, lower), (upper_base, upper) in pairwise(pinned):
        if upper_base < lower_base + lower.size:
            raise ValueError(
                f"pinned pools {lower.id!r} and {upper.id!r} already overlap"
            )
