#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass

from omnimalloc._cpp import TabuSearchAllocatorCpp as _TabuSearchAllocatorCpp
from omnimalloc._cpp import TabuSearchConfig as _TabuSearchConfig
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


@dataclass(frozen=True)
class TabuSearchConfig:
    """Neighborhood size, iteration budget, and tabu memory for TabuSearchAllocator."""

    seed: int = 42
    max_iterations: int = 500
    neighborhood_size: int = 20
    tabu_tenure: int = 15
    max_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.max_iterations <= 0:
            raise ValueError(
                f"max_iterations must be positive, got {self.max_iterations}"
            )
        if self.neighborhood_size <= 0:
            raise ValueError(
                f"neighborhood_size must be positive, got {self.neighborhood_size}"
            )
        if self.tabu_tenure <= 0:
            raise ValueError(f"tabu_tenure must be positive, got {self.tabu_tenure}")
        if self.max_seconds < 0:
            raise ValueError(
                f"max_seconds must be non-negative, got {self.max_seconds}"
            )

    def to_cpp_config(self) -> _TabuSearchConfig:
        return _TabuSearchConfig(
            seed=self.seed,
            max_iterations=self.max_iterations,
            neighborhood_size=self.neighborhood_size,
            tabu_tenure=self.tabu_tenure,
            max_seconds=self.max_seconds,
        )


class TabuSearchAllocator(BaseAllocator):
    """Tabu search over first-fit placement orders, run entirely in C++.

    Each iteration samples a neighborhood of candidate swaps between a
    currently-peak allocation and an earlier temporal neighbor, and moves to
    the best-scoring candidate that is not tabu (or, per the aspiration
    criterion, a tabu one that beats the best solution found so far). The
    swap just made is then forbidden from being immediately reversed for
    `tabu_tenure` iterations, which helps the search escape local optima
    without cycling between the same two orders. Each iteration evaluates
    `neighborhood_size` full placements, so `max_seconds` (default 2s) bounds
    wall-clock time as the input grows, independent of `max_iterations`.
    """

    def __init__(self, config: TabuSearchConfig | None = None) -> None:
        self._config = config or TabuSearchConfig()

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations
        cpp_allocator = _TabuSearchAllocatorCpp(self._config.to_cpp_config())
        return tuple(cpp_allocator.allocate(list(allocations)))
