#
# SPDX-License-Identifier: Apache-2.0
#

import random

from .tiling_base import TilingBase, _Tile


class TilingSource(TilingBase):
    """Generate hard packing problems via recursive guillotine cuts.

    Every split runs edge to edge, so the optimum is always recoverable by
    recursive divide-and-conquer (the guillotine sibling of ``PinwheelSource``).
    ``mem_cut_prob`` tunes temporal contention (and thus hardness) by biasing
    cuts toward the memory axis. See ``TilingBase`` for the construction and
    optimality guarantee.
    """

    def __init__(
        self,
        num_allocations: int = 128,
        capacity: int = 1024 * 1024,
        makespan: int = 1024 * 1024,
        min_size: int = 1024,
        min_duration: int = 1,
        mem_cut_prob: float = 0.5,
        seed: int | None = 42,
    ) -> None:
        if capacity < min_size:
            raise ValueError("capacity must be >= min_size")
        if makespan < min_duration:
            raise ValueError("makespan must be >= min_duration")
        if not 0.0 <= mem_cut_prob <= 1.0:
            raise ValueError("mem_cut_prob must be in [0, 1]")
        super().__init__(
            num_allocations, capacity, makespan, min_size, min_duration, seed
        )
        self.mem_cut_prob = mem_cut_prob

    def _can_split_time(self, tile: _Tile) -> bool:
        return tile.end - tile.start >= 2 * self.min_duration

    def _can_split_mem(self, tile: _Tile) -> bool:
        return tile.size >= 2 * self.min_size

    def _can_split(self, tile: _Tile) -> bool:
        return self._can_split_time(tile) or self._can_split_mem(tile)

    def _split(self, tile: _Tile, rng: random.Random) -> list[_Tile]:
        can_mem = self._can_split_mem(tile)
        can_time = self._can_split_time(tile)

        cut_mem = rng.random() < self.mem_cut_prob if can_mem and can_time else can_mem

        if cut_mem:
            cut = rng.randint(
                tile.offset + self.min_size,
                tile.offset + tile.size - self.min_size,
            )
            left = _Tile(tile.start, tile.end, tile.offset, cut - tile.offset)
            right = _Tile(tile.start, tile.end, cut, tile.offset + tile.size - cut)
        else:
            cut = rng.randint(
                tile.start + self.min_duration,
                tile.end - self.min_duration,
            )
            left = _Tile(tile.start, cut, tile.offset, tile.size)
            right = _Tile(cut, tile.end, tile.offset, tile.size)

        return [left, right]
