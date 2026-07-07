#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass

from omnimalloc._cpp import TelamallocAllocatorCpp as _TelamallocAllocatorCpp
from omnimalloc._cpp import TelamallocConfig as _TelamallocConfig
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


@dataclass(frozen=True)
class TelamallocConfig:
    """Search budgets for TelamallocAllocator."""

    seed: int = 42
    max_backtracks: int = 10000
    max_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.max_backtracks < 0:
            raise ValueError(
                f"max_backtracks must be non-negative, got {self.max_backtracks}"
            )
        if self.max_seconds < 0:
            raise ValueError(
                f"max_seconds must be non-negative, got {self.max_seconds}"
            )

    def to_cpp_config(self) -> _TelamallocConfig:
        return _TelamallocConfig(
            seed=self.seed,
            max_backtracks=self.max_backtracks,
            max_seconds=self.max_seconds,
        )


class TelamallocAllocator(BaseAllocator):
    """TelaMalloc-style allocator (Maas et al., ASPLOS 2023), run entirely in C++.

    The paper packs buffers below a fixed on-chip capacity by interleaving a
    tiered buffer-ordering heuristic with constraint-solver feedback and
    conflict-directed backtracking; it ships in Google's TPUv4 and Pixel 6.
    No reference implementation is public, so this is an adaptation that
    minimizes peak memory instead of testing satisfiability: independent
    phases (connected components of the temporal-overlap graph) are each
    packed in the paper's tiered order (longest lifetime, then largest size)
    with min-conflict eviction as minor backtracking and squeaky-wheel
    restarts as major backtracking, while a binary search drives each phase's
    capacity from a first-fit incumbent down toward its load lower bound. An
    occasional seeded random-walk repair breaks min-conflict cycles; results
    are deterministic for a fixed `seed`, and setting `max_seconds` (default
    2s) to 0 makes them reproducible across machines via `max_backtracks`
    alone.
    """

    def __init__(self, config: TelamallocConfig | None = None) -> None:
        self._config = config or TelamallocConfig()

    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        if not allocations:
            return allocations
        cpp_allocator = _TelamallocAllocatorCpp(self._config.to_cpp_config())
        return tuple(cpp_allocator.allocate(list(allocations)))
