#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass

from omnimalloc._cpp import (
    SimulatedAnnealingAllocatorCpp as _SimulatedAnnealingAllocatorCpp,
)
from omnimalloc._cpp import SimulatedAnnealingConfig as _SimulatedAnnealingConfig
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import ensure_valid_timeout
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


@dataclass(frozen=True)
class SimulatedAnnealingConfig:
    """Cooling schedule and iteration budget for `SimulatedAnnealingAllocator`."""

    seed: int = DEFAULT_SEED
    max_iterations: int = 3000
    # Percent memory worsening accepted with probability 1/e at iteration 0;
    # decays geometrically by `cooling_rate` every iteration.
    initial_temperature: float = 3.0
    cooling_rate: float = 0.998
    # Wall-clock budget in seconds; None disables it.
    timeout: float | None = DEFAULT_TIMEOUT

    def __post_init__(self) -> None:
        if self.max_iterations <= 0:
            raise ValueError(
                f"max_iterations must be positive, got {self.max_iterations}"
            )
        if self.initial_temperature < 0:
            raise ValueError(
                f"initial_temperature must be non-negative, "
                f"got {self.initial_temperature}"
            )
        if not 0.0 < self.cooling_rate <= 1.0:
            raise ValueError(f"cooling_rate must be in (0, 1], got {self.cooling_rate}")
        ensure_valid_timeout(self.timeout)

    def to_cpp_config(self) -> _SimulatedAnnealingConfig:
        return _SimulatedAnnealingConfig(
            seed=self.seed,
            max_iterations=self.max_iterations,
            initial_temperature=self.initial_temperature,
            cooling_rate=self.cooling_rate,
            timeout=self.timeout,
        )


class SimulatedAnnealingAllocator(BaseAllocator):
    """Simulated annealing over first-fit placement orders, run entirely in C++.

    Repeatedly swaps a peak allocation with an earlier temporal neighbor,
    accepting improving swaps outright and worsening ones with a probability
    that anneals to zero over `max_iterations`. Because the whole search loop
    (including every candidate placement) runs natively, it evaluates far more
    candidates per second than an equivalent Python-orchestrated local search
    such as `HillClimbAllocator`. Each iteration re-evaluates a full placement
    of every allocation, so `timeout` (default 3s) bounds wall-clock time
    as the input grows, independent of `max_iterations`.
    """

    supports_vector_time = True

    def __init__(self, config: SimulatedAnnealingConfig | None = None) -> None:
        self._config = config or SimulatedAnnealingConfig()

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        cpp_allocator = _SimulatedAnnealingAllocatorCpp(self._config.to_cpp_config())
        return tuple(cpp_allocator.allocate(list(allocations)))
