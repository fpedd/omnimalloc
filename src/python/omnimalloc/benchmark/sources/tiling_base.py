#
# SPDX-License-Identifier: Apache-2.0
#

import random
from abc import abstractmethod
from dataclasses import dataclass

from omnimalloc.primitives import Allocation, Pool

from .base import BaseSource


@dataclass(frozen=True)
class _Tile:
    """A leaf rectangle in the (time x memory) plane.

    ``offset`` is the leaf's memory position in the ground-truth packing; it is
    discarded when handed to an allocator but kept for validation/reference.
    """

    start: int
    end: int
    offset: int
    size: int


class TilingBase(BaseSource):
    """Reverse-constructed packing problems with a known, tight optimum.

    A ``capacity x makespan`` rectangle is recursively split into leaf tiles
    until ``num_allocations`` is reached; each leaf becomes one allocation with
    its time span and memory height. Because the leaves perfectly tile the
    rectangle, peak pressure equals ``capacity`` exactly, so ``capacity`` is a
    provably achievable optimum. Subclasses supply the cut geometry via
    ``_can_split`` and ``_split``; sweeping ``num_allocations`` yields a
    difficulty ladder.
    """

    def __init__(
        self,
        num_allocations: int,
        capacity: int,
        makespan: int,
        min_size: int,
        min_duration: int,
        seed: int | None,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if min_size <= 0:
            raise ValueError("min_size must be positive")
        if min_duration <= 0:
            raise ValueError("min_duration must be positive")
        self.capacity = capacity
        self.makespan = makespan
        self.min_size = min_size
        self.min_duration = min_duration
        self.seed = seed

    @abstractmethod
    def _can_split(self, tile: _Tile) -> bool: ...

    @abstractmethod
    def _split(self, tile: _Tile, rng: random.Random) -> list[_Tile]:
        """Split a tile into children that exactly tile it."""

    def _build_tiles(self, num: int, rng: random.Random) -> list[_Tile]:
        if num <= 0:
            return []
        tiles = [_Tile(0, self.makespan, 0, self.capacity)]
        while len(tiles) < num:
            splittable = [i for i, t in enumerate(tiles) if self._can_split(t)]
            if not splittable:
                raise ValueError(f"cannot reach {num} allocations with given minima")
            # Weight by area so larger tiles split first, yielding balanced
            # layouts rather than a few dominant blocks plus many slivers.
            weights = [
                (tiles[i].end - tiles[i].start) * tiles[i].size for i in splittable
            ]
            idx = rng.choices(splittable, weights=weights, k=1)[0]
            tiles[idx : idx + 1] = self._split(tiles[idx], rng)
        return tiles

    def _tile_allocations(
        self, num_allocations: int | None, skip: int, with_offsets: bool
    ) -> tuple[Allocation, ...]:
        num = num_allocations if num_allocations is not None else self.num_allocations
        seed = None if self.seed is None else self.seed + skip
        tiles = self._build_tiles(num, random.Random(seed))
        return tuple(
            Allocation(
                id=skip + i,
                size=t.size,
                start=t.start,
                end=t.end,
                offset=t.offset if with_offsets else None,
            )
            for i, t in enumerate(tiles)
        )

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        return self._tile_allocations(num_allocations, skip, with_offsets=False)

    def get_ground_truth_pool(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> Pool:
        """Return the pool with the construction (zero-fragmentation) offsets."""
        if self.seed is None:
            raise ValueError("ground truth requires a fixed seed")
        allocations = self._tile_allocations(num_allocations, skip, with_offsets=True)
        return Pool(id=f"{self.name()}_ground_truth", allocations=allocations)
