#
# SPDX-License-Identifier: Apache-2.0
#

import random
from typing import ClassVar

from omnimalloc.common.constants import DEFAULT_SEED, KB, MB

from .tiling_base import TilingBase, _Tile


class TilingSource(TilingBase):
    """Generate hard packing problems via recursive guillotine cuts.

    Every split runs edge to edge, so divide-and-conquer always recovers the
    optimum. ``mem_cut_prob`` biases cuts toward the memory axis.
    """

    _label_fields: ClassVar[tuple[str, ...]] = (
        "capacity",
        "makespan",
        "size_min",
        "duration_min",
        "seed",
        "mem_cut_prob",
    )

    def __init__(
        self,
        num_allocations: int = 128,
        capacity: int = MB,
        makespan: int = 1024 * 1024,
        size_min: int = KB,
        duration_min: int = 1,
        mem_cut_prob: float = 0.5,
        seed: int | None = DEFAULT_SEED,
    ) -> None:
        if capacity < size_min:
            raise ValueError("capacity must be >= size_min")
        if makespan < duration_min:
            raise ValueError("makespan must be >= duration_min")
        if not 0.0 <= mem_cut_prob <= 1.0:
            raise ValueError("mem_cut_prob must be in [0, 1]")
        super().__init__(
            num_allocations, capacity, makespan, size_min, duration_min, seed
        )
        self.mem_cut_prob = mem_cut_prob

    def _can_split_time(self, tile: _Tile[int]) -> bool:
        return tile.end - tile.start >= 2 * self.duration_min

    def _can_split_mem(self, tile: _Tile[int]) -> bool:
        return tile.size >= 2 * self.size_min

    def _can_split(self, tile: _Tile[int]) -> bool:
        return self._can_split_time(tile) or self._can_split_mem(tile)

    def _split(self, tile: _Tile[int], rng: random.Random) -> list[_Tile[int]]:
        can_mem = self._can_split_mem(tile)
        can_time = self._can_split_time(tile)

        cut_mem = rng.random() < self.mem_cut_prob if can_mem and can_time else can_mem

        if cut_mem:
            cut = rng.randint(
                tile.offset + self.size_min,
                tile.offset + tile.size - self.size_min,
            )
            left = _Tile(tile.start, tile.end, tile.offset, cut - tile.offset)
            right = _Tile(tile.start, tile.end, cut, tile.offset + tile.size - cut)
        else:
            cut = rng.randint(
                tile.start + self.duration_min,
                tile.end - self.duration_min,
            )
            left = _Tile(tile.start, cut, tile.offset, tile.size)
            right = _Tile(cut, tile.end, tile.offset, tile.size)

        return [left, right]
