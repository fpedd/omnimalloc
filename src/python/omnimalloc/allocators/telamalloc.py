#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import dataclass

from omnimalloc._cpp import TelamallocAllocatorCpp as _TelamallocAllocatorCpp
from omnimalloc._cpp import TelamallocConfig as _TelamallocConfig
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import ensure_valid_timeout
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


@dataclass(frozen=True)
class TelamallocConfig:
    """Search budgets for TelamallocAllocator."""

    seed: int = DEFAULT_SEED
    # Eviction (backtrack) budget per capacity attempt; an attempt that
    # exhausts it reports the capacity as unreachable.
    max_backtracks: int = 10000
    # Wall-clock budget in seconds; None disables it.
    timeout: float | None = DEFAULT_TIMEOUT

    def __post_init__(self) -> None:
        if self.max_backtracks < 0:
            raise ValueError(
                f"max_backtracks must be non-negative, got {self.max_backtracks}"
            )
        ensure_valid_timeout(self.timeout)

    def to_cpp_config(self) -> _TelamallocConfig:
        return _TelamallocConfig(
            seed=self.seed,
            max_backtracks=self.max_backtracks,
            timeout=self.timeout,
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
    are deterministic for a fixed `seed`, and setting `timeout` (default
    3s) to 0 makes them reproducible across machines via `max_backtracks`
    alone.
    """

    # The phase decomposition and load bounds sweep a linear timeline
    supports_vector_time = False

    def __init__(self, config: TelamallocConfig | None = None) -> None:
        self._config = config or TelamallocConfig()

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        cpp_allocator = _TelamallocAllocatorCpp(self._config.to_cpp_config())
        return tuple(cpp_allocator.allocate(list(allocations)))
