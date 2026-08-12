#
# SPDX-License-Identifier: Apache-2.0
#

import random
from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, Generic, TypeVar

from omnimalloc.primitives import Allocation, IdType, Pool, TimePoint

from .base import BaseSource

TimeT_co = TypeVar("TimeT_co", bound=TimePoint, covariant=True)


@dataclass(frozen=True)
class _Tile(Generic[TimeT_co]):
    """A leaf rectangle in the (time x memory) plane.

    The split recursion works on a scalar timeline; vector-clock sources re-time
    tiles first. ``offset`` is the ground-truth position, kept for validation.
    """

    start: TimeT_co
    end: TimeT_co
    offset: int
    size: int


class TilingBase(BaseSource):
    """Reverse-constructed packing problems with a known, tight optimum.

    A ``capacity x makespan`` rectangle splits recursively into leaf tiles, one
    allocation each. They tile it perfectly, so ``capacity`` is the optimum.
    """

    _label_fields: ClassVar[tuple[str, ...]] = (
        "capacity",
        "makespan",
        "size_min",
        "duration_min",
        "seed",
    )

    def __init__(
        self,
        num_allocations: int,
        capacity: int,
        makespan: int,
        size_min: int,
        duration_min: int,
        seed: int | None,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if duration_min <= 0:
            raise ValueError("duration_min must be positive")
        self.capacity = capacity
        self.makespan = makespan
        self.size_min = size_min
        self.duration_min = duration_min
        self.seed = seed

    @property
    def memory_capacity(self) -> int:
        """Exactly what an optimal placement of every pool needs, and no more.

        The leaves tile the rectangle, so a memory of `num_pools` optima is
        barely feasible. Lower it to build an infeasible instance.
        """
        return self.num_pools * self.capacity

    @abstractmethod
    def _can_split(self, tile: _Tile[int]) -> bool: ...

    @abstractmethod
    def _split(self, tile: _Tile[int], rng: random.Random) -> list[_Tile[int]]:
        """Split a tile into children that exactly tile it."""

    def _build_tiles(
        self, num: int, rng: random.Random, capacity: int | None = None
    ) -> list[_Tile[int]]:
        if num <= 0:
            return []
        tiles = [_Tile(0, self.makespan, 0, capacity or self.capacity)]
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
        if len(tiles) != num:
            raise ValueError(
                f"cannot reach exactly {num} allocations, nearest is {len(tiles)}"
            )
        return tiles

    def _variant_rng(self, skip: int) -> random.Random:
        """Deterministic per-variant stream: same seed and skip, same problem."""
        return random.Random(None if self.seed is None else self.seed + skip)

    def _placed_tiles(self, num: int, skip: int) -> Sequence[_Tile[TimePoint]]:
        """One placed tile per allocation to generate; the subclass hook."""
        return self._build_tiles(num, self._variant_rng(skip))

    def _tile_allocations(
        self, num_allocations: int | None, skip: int, with_offsets: bool
    ) -> tuple[Allocation, ...]:
        num = num_allocations if num_allocations is not None else self.num_allocations
        return tuple(
            Allocation(
                id=skip + i,
                size=tile.size,
                start=tile.start,
                end=tile.end,
                offset=tile.offset if with_offsets else None,
            )
            for i, tile in enumerate(self._placed_tiles(num, skip))
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

    def get_known_optimum(self, variant_id: IdType | None = None) -> int | None:
        """Peak of the construction packing, which is optimal by tiling.

        Unknown without a fixed seed: the ground truth is then a different
        random packing than the one that was benchmarked.
        """
        if self.seed is None:
            return None
        num = variant_id if isinstance(variant_id, int) else None
        return self.get_ground_truth_pool(num).size
