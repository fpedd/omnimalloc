#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc._cpp import FirstFitPlacer
from omnimalloc.common.constants import DEFAULT_SEED, DEFAULT_TIMEOUT
from omnimalloc.common.deadline import (
    deadline_expired,
    ensure_valid_timeout,
    make_deadline,
)
from omnimalloc.common.validation import ensure_positive
from omnimalloc.primitives import Allocation

from .base import BaseAllocator
from .utils import ensure_seed


class RandomAllocator(BaseAllocator):
    """Best of `num_trials` random first-fit orders; `timeout` binds."""

    supports_vector_time = True
    supports_pinned = True

    def __init__(
        self,
        seed: int = DEFAULT_SEED,
        num_trials: int = 100,
        timeout: float | None = DEFAULT_TIMEOUT,
    ) -> None:
        ensure_seed(seed)
        ensure_positive(num_trials, "num_trials")
        ensure_valid_timeout(timeout)
        self._seed = seed
        self._num_trials = num_trials
        self._timeout = timeout

    def _allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        deadline = make_deadline(self._timeout)
        # Fresh RNG per call: repeated calls on one instance are deterministic
        rng = random.Random(self._seed)
        placer = FirstFitPlacer(allocations)
        order = list(range(len(allocations)))
        rng.shuffle(order)
        best_order, best_peak = list(order), placer.peak(order)

        for _ in range(self._num_trials - 1):
            if deadline_expired(deadline):
                break
            rng.shuffle(order)
            peak = placer.peak(order)
            if peak < best_peak:
                best_order, best_peak = list(order), peak

        return tuple(placer.place(best_order))
