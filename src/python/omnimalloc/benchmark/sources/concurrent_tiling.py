#
# SPDX-License-Identifier: Apache-2.0
#

import random
from bisect import bisect_right
from typing import ClassVar

from omnimalloc.common.constants import DEFAULT_SEED, KB, MB
from omnimalloc.primitives import TimePoint, VectorClock

from .tiling import TilingSource
from .tiling_base import _Tile

# Per-thread receive history: parallel (local times, merged clock snapshots)
_SyncHistory = tuple[list[int], list[VectorClock]]


class ConcurrentTilingSource(TilingSource):
    """Guillotine tilings sharded across partially-synchronized workers.

    Each worker tiles its own memory band over a private timeline joined by
    ``num_syncs`` messages, so ``capacity`` stays a provably achievable optimum.
    """

    _label_fields: ClassVar[tuple[str, ...]] = (
        "capacity",
        "makespan",
        "size_min",
        "duration_min",
        "seed",
        "mem_cut_prob",
        "num_threads",
        "num_syncs",
    )

    def __init__(
        self,
        num_allocations: int = 128,
        num_threads: int = 2,
        num_syncs: int = 16,
        capacity: int = MB,
        makespan: int = 1024 * 1024,
        size_min: int = KB,
        duration_min: int = 1,
        mem_cut_prob: float = 0.5,
        seed: int | None = DEFAULT_SEED,
    ) -> None:
        if num_threads <= 0:
            raise ValueError("num_threads must be positive")
        if num_allocations < num_threads:
            raise ValueError("num_allocations must be >= num_threads")
        if capacity % num_threads:
            raise ValueError("capacity must be divisible by num_threads")
        if capacity // num_threads < size_min:
            raise ValueError("per-thread capacity must be >= size_min")
        if not 0 <= num_syncs < makespan:
            raise ValueError("num_syncs must be in [0, makespan)")
        super().__init__(
            num_allocations,
            capacity,
            makespan,
            size_min,
            duration_min,
            mem_cut_prob,
            seed,
        )
        self.num_threads = num_threads
        self.num_syncs = num_syncs

    def _placed_tiles(self, num: int, skip: int) -> list[_Tile[TimePoint]]:
        if num < self.num_threads:
            # An empty band would break the capacity-is-optimal guarantee
            raise ValueError("num_allocations must be >= num_threads")
        rng = self._variant_rng(skip)
        histories = self._simulate_syncs(rng)
        band_capacity = self.capacity // self.num_threads

        placed: list[_Tile[TimePoint]] = []
        for thread in range(self.num_threads):
            count = num // self.num_threads
            if thread < num % self.num_threads:
                count += 1
            placed.extend(
                _Tile(
                    start=self._project(histories[thread], thread, tile.start),
                    end=self._project(histories[thread], thread, tile.end),
                    offset=tile.offset + thread * band_capacity,
                    size=tile.size,
                )
                for tile in self._build_tiles(count, rng, capacity=band_capacity)
            )
        return placed

    def _simulate_syncs(self, rng: random.Random) -> list[_SyncHistory]:
        """Deliver random sync messages, max-merging the receiver's clock."""
        knowledge = [[0] * self.num_threads for _ in range(self.num_threads)]
        histories: list[_SyncHistory] = [([], []) for _ in range(self.num_threads)]
        if self.num_threads < 2 or not self.num_syncs:
            return histories
        # All workers share the local step scale, so delivering messages in
        # instant order is a causally consistent execution.
        for instant in sorted(rng.sample(range(1, self.makespan), self.num_syncs)):
            sender, receiver = rng.sample(range(self.num_threads), 2)
            knowledge[sender][sender] = instant
            merged = knowledge[receiver]
            for component, value in enumerate(knowledge[sender]):
                merged[component] = max(merged[component], value)
            times, snapshots = histories[receiver]
            times.append(instant)
            snapshots.append(tuple(merged))
        return histories

    def _project(
        self, history: _SyncHistory, thread: int, local_time: int
    ) -> VectorClock:
        """Vector clock of `thread` at `local_time`: own step + last receive."""
        times, snapshots = history
        index = bisect_right(times, local_time) - 1
        clock = list(snapshots[index]) if index >= 0 else [0] * self.num_threads
        clock[thread] = local_time
        return tuple(clock)
