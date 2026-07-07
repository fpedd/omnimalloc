#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass

from omnimalloc._cpp import (
    SimulatedAnnealingAllocatorCpp as _SimulatedAnnealingAllocatorCpp,
)
from omnimalloc._cpp import SimulatedAnnealingConfig as _SimulatedAnnealingConfig
from omnimalloc.primitives import Allocation

from .base import DEFAULT_MAX_SECONDS, BaseAllocator


@dataclass(frozen=True)
class SimulatedAnnealingConfig:
    """Cooling schedule and iteration budget for `SimulatedAnnealingAllocator`."""

    seed: int = 42
    max_iterations: int = 3000
    initial_temperature: float = 3.0
    cooling_rate: float = 0.998
    max_seconds: float = DEFAULT_MAX_SECONDS

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
        if self.max_seconds < 0:
            raise ValueError(
                f"max_seconds must be non-negative, got {self.max_seconds}"
            )

    def to_cpp_config(self) -> _SimulatedAnnealingConfig:
        return _SimulatedAnnealingConfig(
            seed=self.seed,
            max_iterations=self.max_iterations,
            initial_temperature=self.initial_temperature,
            cooling_rate=self.cooling_rate,
            max_seconds=self.max_seconds,
        )


class SimulatedAnnealingAllocator(BaseAllocator):
    """Simulated annealing over first-fit placement orders, run entirely in C++.

    Repeatedly swaps a peak allocation with an earlier temporal neighbor,
    accepting improving swaps outright and worsening ones with a probability
    that anneals to zero over `max_iterations`. Because the whole search loop
    (including every candidate placement) runs natively, it evaluates far more
    candidates per second than an equivalent Python-orchestrated local search
    such as `HillClimbAllocator`. Each iteration re-evaluates a full placement
    of every allocation, so `max_seconds` (default 3s) bounds wall-clock time
    as the input grows, independent of `max_iterations`.
    """

    def __init__(self, config: SimulatedAnnealingConfig | None = None) -> None:
        self._config = config or SimulatedAnnealingConfig()

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations
        cpp_allocator = _SimulatedAnnealingAllocatorCpp(self._config.to_cpp_config())
        return tuple(cpp_allocator.allocate(list(allocations)))
