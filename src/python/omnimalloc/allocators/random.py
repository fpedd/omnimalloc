#
# SPDX-License-Identifier: Apache-2.0
#

import sys

import numpy as np

from omnimalloc.primitives import Allocation

from .greedy import GreedyAllocator


class RandomAllocator(GreedyAllocator):
    """Randomized allocator that tries multiple random orders and picks the best."""

    def __init__(self, num_trials: int = 100, seed: int = 42) -> None:
        self._seed = seed
        self._num_trials = num_trials
        self._rng = np.random.RandomState(self._seed)

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations

        if self._num_trials <= 0:
            return super().allocate(allocations)

        best_allocation: tuple[Allocation, ...] | None = None
        best_peak_memory = sys.maxsize

        allocs_array = np.array(allocations)

        for _ in range(self._num_trials):
            permuted_indices = self._rng.permutation(len(allocations))
            permuted_allocs = tuple(allocs_array[permuted_indices])

            result = super().allocate(permuted_allocs)

            assert all(alloc.height is not None for alloc in result)
            heights = [alloc.height for alloc in result if alloc.height is not None]
            peak_memory = max(heights) if heights else 0

            if peak_memory < best_peak_memory:
                best_peak_memory = peak_memory
                best_allocation = result

        assert best_allocation is not None
        return best_allocation

    def reset(self) -> None:
        self._rng = np.random.RandomState(self._seed)
