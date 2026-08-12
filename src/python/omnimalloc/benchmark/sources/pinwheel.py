#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc.common.constants import DEFAULT_SEED, KB, MB

from .tiling_base import TilingBase, _Tile


class PinwheelSource(TilingBase):
    """Generate non-guillotine packing problems with a known, tight optimum.

    Each tile splits into a central rectangle ringed by four blades, so no
    straight cut crosses it and decomposition cannot shortcut to the optimum.
    """

    def __init__(
        self,
        num_allocations: int = 129,
        capacity: int = MB,
        makespan: int = 1024 * 1024,
        size_min: int = KB,
        duration_min: int = 1,
        seed: int | None = DEFAULT_SEED,
    ) -> None:
        if capacity < 3 * size_min:
            raise ValueError("capacity must be >= 3 * size_min to seat a pinwheel")
        if makespan < 3 * duration_min:
            raise ValueError("makespan must be >= 3 * duration_min to seat a pinwheel")
        super().__init__(
            num_allocations, capacity, makespan, size_min, duration_min, seed
        )

    def _can_split(self, tile: _Tile[int]) -> bool:
        return (
            tile.end - tile.start >= 3 * self.duration_min
            and tile.size >= 3 * self.size_min
        )

    def _split(self, tile: _Tile[int], rng: random.Random) -> list[_Tile[int]]:
        """Split a tile into a five-piece pinwheel (center + four blades)."""
        t0, t1, m0 = tile.start, tile.end, tile.offset
        width, height = t1 - t0, tile.size
        m1 = m0 + height
        # Blade thicknesses; bounds keep all five children >= the minima.
        p = rng.randint(self.duration_min, (width - self.duration_min) // 2)
        q = rng.randint(self.size_min, (height - self.size_min) // 2)
        return [
            _Tile(t0, t1 - p, m0, q),  # bottom
            _Tile(t1 - p, t1, m0, height - q),  # right
            _Tile(t0 + p, t1, m1 - q, q),  # top
            _Tile(t0, t0 + p, m0 + q, height - q),  # left
            _Tile(t0 + p, t1 - p, m0 + q, height - 2 * q),  # center
        ]
