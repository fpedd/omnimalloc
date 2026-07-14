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
        if num_trials < 0:
            raise ValueError(f"num_trials must be non-negative, got {num_trials}")
        self._seed = seed
        self._num_trials = num_trials

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if self._num_trials <= 0:
            return super()._allocate(allocations)

        # Fresh RNG per call: repeated calls on one instance are deterministic
        rng = random.Random(self._seed)
        placer = FirstFitPlacer(list(allocations))
        order = list(range(len(allocations)))
        best_order: list[int] | None = None
        best_peak = 0

        for _ in range(self._num_trials):
            rng.shuffle(order)
            peak = placer.evaluate(order)
            if best_order is None or peak < best_peak:
                best_order, best_peak = list(order), peak

        assert best_order is not None
        return tuple(placer.place(best_order))
