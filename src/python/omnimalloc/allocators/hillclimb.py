#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT

from .simulated_annealing import SimulatedAnnealingAllocator


class HillClimbAllocator(SimulatedAnnealingAllocator):
    """Hill climbing over first-fit placement orders, run entirely in C++.

    The zero-temperature simulated annealing kernel: repeatedly swaps a peak
    allocation with an earlier temporal neighbor, keeping only non-worsening
    moves. `timeout` binds.
    """

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        max_iterations: int = 3000,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        super().__init__(
            seed=seed,
            max_iterations=max_iterations,
            initial_temperature=0.0,
            cooling_rate=1.0,
            timeout=timeout,
        )
