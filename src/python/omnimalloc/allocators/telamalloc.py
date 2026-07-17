#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc._cpp import telamalloc_place
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import ensure_valid_timeout
from omnimalloc.common.validation import ensure_non_negative
from omnimalloc.primitives import Allocation

from .base import BaseAllocator


class TelamallocAllocator(BaseAllocator):
    """TelaMalloc-style allocator (Maas et al., ASPLOS 2023), run entirely in C++.

    The paper packs buffers below a fixed on-chip capacity by interleaving a
    tiered buffer-ordering heuristic with constraint-solver feedback and
    conflict-directed backtracking; it ships in Google's TPUv4 and Pixel 6.
    No reference implementation is public, so this is an adaptation that
    minimizes peak memory instead of testing satisfiability: independent
    phases (connected components of the conflict graph) are each packed in
    the paper's tiered order (longest lifetime, then largest size) with
    min-conflict eviction as minor backtracking and squeaky-wheel restarts
    as major backtracking, while a binary search drives each phase's
    capacity from a first-fit incumbent down toward its load lower bound.
    `max_backtracks` is the eviction budget per capacity attempt; an attempt
    that exhausts it reports the capacity as unreachable. An occasional
    seeded random-walk repair breaks min-conflict cycles; results are
    deterministic for a fixed `seed`, and `timeout=None` (default 3s)
    makes them reproducible across machines via `max_backtracks` alone.
    """

    # The phase decomposition and load bounds sweep a linear timeline
    supports_vector_time = False

    def __init__(
        self,
        *,
        seed: int = DEFAULT_SEED,
        max_backtracks: int = 10000,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        ensure_non_negative(max_backtracks, "max_backtracks")
        ensure_valid_timeout(timeout)

        self._seed = seed
        self._max_backtracks = max_backtracks
        self._timeout = timeout

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return tuple(
            telamalloc_place(
                allocations,
                seed=self._seed,
                max_backtracks=self._max_backtracks,
                timeout=self._timeout,
            )
        )
