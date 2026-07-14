#
# SPDX-License-Identifier: Apache-2.0
#

import random
from math import isqrt
from typing import Final

from omnimalloc.primitives import Allocation, VectorClock

from .base import BaseSource

# Ordered from loosest to tightest thread coupling
SYNC_PATTERNS: Final[tuple[str, ...]] = (
    "independent",
    "sparse",
    "ring",
    "groups",
    "barrier",
    "fork_join",
    "dense",
)


class SyncPatternSource(BaseSource):
    """Vector-clock lifetimes from simulated threads with a chosen sync topology.

    ``num_threads`` workers tick a shared local step scale while sync events
    merge their clocks according to ``pattern``; each allocation is born and
    dies at two snapshots of one worker's clock. The patterns span the
    coupling spectrum:

    - ``independent``: no syncs; fully concurrent timelines
    - ``sparse``: one random pairwise message every ``sync_period`` steps
      (long-running, loosely coupled concurrency)
    - ``ring``: each step one thread sends to its right neighbor (pipeline)
    - ``groups``: barriers inside groups of ``group_size`` threads every
      ``sync_period`` steps, a global barrier every fourth period
      (hierarchical synchronization)
    - ``barrier``: global barrier every ``sync_period`` steps (phased)
    - ``fork_join``: a coordinator broadcasts at round start and collects at
      round end (concurrency confined to short rounds)
    - ``dense``: every thread receives from a random peer every step
      (near-lockstep)
    """

    def __init__(
        self,
        num_allocations: int = 128,
        num_threads: int = 4,
        pattern: str = "dense",
        steps: int | None = None,
        sync_period: int = 8,
        group_size: int | None = None,
        size_min: int = 1024,
        size_max: int = 1024 * 1024,
        max_lifetime: int | None = None,
        seed: int | None = 42,
    ) -> None:
        if num_threads <= 0:
            raise ValueError("num_threads must be positive")
        if pattern not in SYNC_PATTERNS:
            raise ValueError(f"pattern must be one of {SYNC_PATTERNS}, got {pattern!r}")
        if steps is not None and steps < 2:
            raise ValueError("steps must be >= 2")
        if sync_period <= 0:
            raise ValueError("sync_period must be positive")
        if group_size is not None and group_size <= 0:
            raise ValueError("group_size must be positive")
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if size_max < size_min:
            raise ValueError("size_max must be >= size_min")
        if max_lifetime is not None and max_lifetime <= 0:
            raise ValueError("max_lifetime must be positive")
        super().__init__(num_allocations=num_allocations)
        self.num_threads = num_threads
        self.pattern = pattern
        self.steps = steps
        self.sync_period = sync_period
        self.group_size = group_size or max(2, isqrt(num_threads))
        self.size_min = size_min
        self.size_max = size_max
        self.max_lifetime = max_lifetime
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        num = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(None if self.seed is None else self.seed + skip)
        steps = self.steps or max(4, 2 * num // self.num_threads)
        max_lifetime = self.max_lifetime or max(1, steps // 4)
        snapshots = self._simulate(steps, rng)

        allocations = []
        for i in range(num):
            thread = rng.randrange(self.num_threads)
            birth = rng.randrange(steps - 1)
            death = rng.randint(birth + 1, min(steps - 1, birth + max_lifetime))
            allocations.append(
                Allocation(
                    id=skip + i,
                    size=rng.randint(self.size_min, self.size_max),
                    start=snapshots[thread][birth],
                    end=snapshots[thread][death],
                )
            )
        return tuple(allocations)

    def _simulate(self, steps: int, rng: random.Random) -> list[list[VectorClock]]:
        """Per-thread clock snapshots after each step of the sync pattern."""
        clocks = [[0] * self.num_threads for _ in range(self.num_threads)]
        snapshots: list[list[VectorClock]] = [[] for _ in range(self.num_threads)]
        # All workers tick on the shared step scale, so delivering messages in
        # step order is a causally consistent execution.
        for step in range(1, steps + 1):
            for thread in range(self.num_threads):
                clocks[thread][thread] = step
            self._sync(clocks, step, rng)
            for thread, clock in enumerate(clocks):
                snapshots[thread].append(tuple(clock))
        return snapshots

    def _sync(self, clocks: list[list[int]], step: int, rng: random.Random) -> None:
        if self.num_threads < 2 or self.pattern == "independent":
            return
        if self.pattern == "sparse":
            self._sync_sparse(clocks, step, rng)
        elif self.pattern == "ring":
            sender = step % self.num_threads
            _deliver(clocks, sender, (sender + 1) % self.num_threads)
        elif self.pattern == "groups":
            self._sync_groups(clocks, step)
        elif self.pattern == "barrier":
            if step % self.sync_period == 0:
                _merge(clocks, range(self.num_threads))
        elif self.pattern == "fork_join":
            self._sync_fork_join(clocks, step)
        elif self.pattern == "dense":
            for receiver in range(self.num_threads):
                sender = receiver + rng.randrange(1, self.num_threads)
                _deliver(clocks, sender % self.num_threads, receiver)

    def _sync_sparse(
        self, clocks: list[list[int]], step: int, rng: random.Random
    ) -> None:
        if step % self.sync_period == 0:
            sender, receiver = rng.sample(range(self.num_threads), 2)
            _deliver(clocks, sender, receiver)

    def _sync_groups(self, clocks: list[list[int]], step: int) -> None:
        if step % (4 * self.sync_period) == 0:
            _merge(clocks, range(self.num_threads))
        elif step % self.sync_period == 0:
            for lo in range(0, self.num_threads, self.group_size):
                hi = min(lo + self.group_size, self.num_threads)
                _merge(clocks, range(lo, hi))

    def _sync_fork_join(self, clocks: list[list[int]], step: int) -> None:
        phase = step % self.sync_period
        if phase == 1 % self.sync_period:
            for worker in range(1, self.num_threads):
                _deliver(clocks, 0, worker)
        if phase == 0:
            for worker in range(1, self.num_threads):
                _deliver(clocks, worker, 0)


def _deliver(clocks: list[list[int]], sender: int, receiver: int) -> None:
    """Message receive: max-merge the sender's clock into the receiver's."""
    received = clocks[receiver]
    for lane, value in enumerate(clocks[sender]):
        received[lane] = max(received[lane], value)


def _merge(clocks: list[list[int]], group: range) -> None:
    """Barrier: every thread in the group adopts the group's join."""
    joined = [max(values) for values in zip(*(clocks[t] for t in group), strict=True)]
    for thread in group:
        clocks[thread][:] = joined
