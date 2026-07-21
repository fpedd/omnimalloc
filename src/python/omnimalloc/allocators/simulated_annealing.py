#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import simulated_annealing_place
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import ensure_valid_timeout
from omnimalloc.common.validation import ensure_non_negative, ensure_positive
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class SimulatedAnnealingAllocator(BaseAllocator):
    """Simulated annealing over first-fit placement orders, run entirely in C++.

    Repeatedly swaps a peak allocation with an earlier temporal neighbor,
    accepting improving swaps outright and worsening ones with a probability
    that anneals to zero over `max_iterations`. `initial_temperature` is the
    percent memory worsening accepted with probability 1/e at iteration 0;
    it decays geometrically by `cooling_rate` every iteration. Because the
    whole search loop (including every candidate placement) runs natively,
    it evaluates far more candidates per second than an equivalent
    Python-orchestrated local search such as `HillClimbAllocator`. Each
    iteration re-evaluates a full placement of every allocation, so
    `timeout` (default 3s) bounds wall-clock time as the input grows,
    independent of `max_iterations`.
    """

    supports_vector_time = True

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        max_iterations: int = 3000,
        initial_temperature: float = 3.0,
        cooling_rate: float = 0.998,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        ensure_positive(max_iterations, "max_iterations")
        ensure_non_negative(initial_temperature, "initial_temperature")
        if not 0.0 < cooling_rate <= 1.0:
            raise ValueError(f"cooling_rate must be in (0, 1], got {cooling_rate}")
        ensure_valid_timeout(timeout)

        self._seed = seed
        self._max_iterations = max_iterations
        self._initial_temperature = initial_temperature
        self._cooling_rate = cooling_rate
        self._timeout = timeout

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(
            simulated_annealing_place(
                allocations,
                seed=self._seed,
                max_iterations=self._max_iterations,
                initial_temperature=self._initial_temperature,
                cooling_rate=self._cooling_rate,
                timeout=self._timeout,
            )
        )
