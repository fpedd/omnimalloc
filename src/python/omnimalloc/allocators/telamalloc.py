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

    No reference implementation is public, so this adaptation minimizes peak
    memory instead, packing each conflict-graph component in the tiered order.
    """

    # The phase decomposition and load bounds sweep a linear timeline
    supports_vector_time = False

    def __init__(
        self,
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
