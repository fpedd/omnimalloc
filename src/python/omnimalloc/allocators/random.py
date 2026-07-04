#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.primitives import Allocation

from .greedy import GreedyAllocator


class RandomAllocator(GreedyAllocator):
    """Randomized allocator that tries multiple random orders and picks the best."""

    def __init__(self, num_trials: int = 100, seed: int = 42) -> None:
        self._seed = seed
        self._num_trials = num_trials
        self._rng = random.Random(seed)

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations or self._num_trials <= 0:
            return super().allocate(allocations)

        placer = FirstFitPlacer(list(allocations))
        order = list(range(len(allocations)))
        best_order: list[int] | None = None
        best_peak = 0

        for _ in range(self._num_trials):
            self._rng.shuffle(order)
            peak = placer.evaluate(order)
            if best_order is None or peak < best_peak:
                best_order, best_peak = list(order), peak

        assert best_order is not None
        return tuple(placer.place(best_order))

    def reset(self) -> None:
        self._rng = random.Random(self._seed)
