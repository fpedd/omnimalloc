#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.common.constants import DEFAULT_SEED
from omnimalloc.common.validation import ensure_non_negative
from omnimalloc.primitives import Allocation

from .greedy import GreedyAllocator


class RandomAllocator(GreedyAllocator):
    """Randomized allocator that tries multiple random orders and picks the best."""

    def __init__(self, num_trials: int = 100, seed: int = DEFAULT_SEED) -> None:
        ensure_non_negative(num_trials, "num_trials")
        self._seed = seed
        self._num_trials = num_trials

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if self._num_trials <= 0:
            return super()._allocate(allocations)

        # Fresh RNG per call: repeated calls on one instance are deterministic
        rng = random.Random(self._seed)
        placer = FirstFitPlacer(allocations)
        order = list(range(len(allocations)))
        best_order: list[int] | None = None
        best_peak = 0

        for _ in range(self._num_trials):
            rng.shuffle(order)
            peak = placer.peak(order)
            if best_order is None or peak < best_peak:
                best_order, best_peak = list(order), peak

        assert best_order is not None
        return tuple(placer.place(best_order))
