#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import tabu_search_place
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import ensure_valid_timeout
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class TabuSearchAllocator(BaseAllocator):
    """Tabu search over first-fit placement orders, run entirely in C++.

    Each iteration samples a neighborhood of `neighborhood_size` candidate
    swaps between a currently-peak allocation and an earlier temporal
    neighbor, and moves to the best-scoring candidate that is not tabu (or,
    per the aspiration criterion, a tabu one that beats the best solution
    found so far). The swap just made is then forbidden from being
    immediately reversed for `tabu_tenure` iterations, which helps the
    search escape local optima without cycling between the same two orders.
    Each iteration evaluates `neighborhood_size` full placements, so
    `timeout` (default 3s) bounds wall-clock time as the input grows,
    independent of `max_iterations`.
    """

    supports_vector_time = True

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        max_iterations: int = 500,
        neighborhood_size: int = 20,
        tabu_tenure: int = 15,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        ensure_positive(max_iterations, "max_iterations")
        ensure_positive(neighborhood_size, "neighborhood_size")
        ensure_positive(tabu_tenure, "tabu_tenure")
        ensure_valid_timeout(timeout)

        self._seed = seed
        self._max_iterations = max_iterations
        self._neighborhood_size = neighborhood_size
        self._tabu_tenure = tabu_tenure
        self._timeout = timeout

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(
            tabu_search_place(
                allocations,
                seed=self._seed,
                max_iterations=self._max_iterations,
                neighborhood_size=self._neighborhood_size,
                tabu_tenure=self._tabu_tenure,
                timeout=self._timeout,
            )
        )
