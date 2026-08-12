#
# SPDX-License-Identifier: Apache-2.0
#

import random
from collections.abc import Callable, Sequence
from enum import Enum
from math import isqrt
from typing import ClassVar, Final

from omnimalloc.common.constants import DEFAULT_SEED, KB, MB
from omnimalloc.primitives import Allocation, VectorClock

from .base import BaseSource
from .sizes import SizeDistribution, sample_sizes


class SyncPattern(str, Enum):
    """Ordered from loosest to tightest thread coupling."""

    INDEPENDENT = "independent"
    PAIRS = "pairs"
    SUBSET = "subset"
    SPARSE = "sparse"
    IRREGULAR = "irregular"
    RING = "ring"
    TREE = "tree"
    GROUPS = "groups"
    BARRIER = "barrier"
    FORK_JOIN = "fork_join"
    DENSE = "dense"

    __str__ = str.__str__


SYNC_PATTERNS: Final[tuple[SyncPattern, ...]] = tuple(SyncPattern)


class SyncPatternSource(BaseSource):
    """Vector-clock lifetimes from simulated threads with a chosen sync topology.

    ``num_threads`` workers tick a shared step scale while sync events merge their
    clocks per ``pattern``; ``speed_skew`` staggers them off that scale.
    """

    _label_fields: ClassVar[tuple[str, ...]] = (
        "num_threads",
        "pattern",
        "steps",
        "sync_period",
        "group_size",
        "size_min",
        "size_max",
        "size_distribution",
        "speed_skew",
        "max_lifetime",
        "seed",
    )

    def __init__(
        self,
        num_allocations: int = 128,
        num_threads: int = 4,
        pattern: SyncPattern | str = SyncPattern.DENSE,
        steps: int | None = None,
        sync_period: int = 8,
        group_size: int | None = None,
        size_min: int = KB,
        size_max: int = MB,
        size_distribution: SizeDistribution | str = SizeDistribution.UNIFORM,
        speed_skew: int = 1,
        max_lifetime: int | None = None,
        seed: int | None = DEFAULT_SEED,
    ) -> None:
        if num_threads <= 0:
            raise ValueError("num_threads must be positive")
        if speed_skew <= 0:
            raise ValueError("speed_skew must be positive")
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
        self.pattern = SyncPattern(pattern)
        self.steps = steps
        self.sync_period = sync_period
        self.group_size = group_size
        self.size_min = size_min
        self.size_max = size_max
        self.size_distribution = SizeDistribution(size_distribution)
        self.speed_skew = speed_skew
        self.max_lifetime = max_lifetime
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        num = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(None if self.seed is None else self.seed + skip)
        steps = self.steps or max(4 * self.speed_skew, 2 * num // self.num_threads)
        max_lifetime = self.max_lifetime or max(1, steps // 4)
        snapshots = self._simulate(steps, rng)
        tick_steps, live = self._tick_steps(steps)

        sizes = sample_sizes(
            rng, num, self.size_distribution, self.size_min, self.size_max
        )

        allocations = []
        for i in range(num):
            # Births and deaths land on the thread's own ticks, so a buffer
            # always outlives its birth even where a slow thread stands still
            thread = rng.choice(live)
            own = tick_steps[thread]
            birth = rng.randrange(len(own) - 1)
            death = rng.randint(birth + 1, min(len(own) - 1, birth + max_lifetime))
            allocations.append(
                Allocation(
                    id=skip + i,
                    size=sizes[i],
                    start=snapshots[thread][own[birth]],
                    end=snapshots[thread][own[death]],
                )
            )
        return tuple(allocations)

    def _tick_periods(self) -> list[int]:
        """Steps between two of each thread's own clock ticks."""
        return [1 + thread % self.speed_skew for thread in range(self.num_threads)]

    def _tick_steps(self, steps: int) -> tuple[list[list[int]], list[int]]:
        """Snapshot indices where each thread's clock advanced, and which tick twice.

        Only a thread that ticks at least twice can hold a buffer, so the
        second list is what births may draw from.
        """
        per_thread = [
            list(range(period - 1, steps, period)) for period in self._tick_periods()
        ]
        live = [t for t, own in enumerate(per_thread) if len(own) >= 2]
        if not live:
            raise ValueError(
                f"no thread ticks twice in {steps} steps at "
                f"speed_skew={self.speed_skew}; raise steps"
            )
        return per_thread, live

    def _simulate(self, steps: int, rng: random.Random) -> list[list[VectorClock]]:
        """Per-thread clock snapshots after each step of the sync pattern."""
        clocks = [[0] * self.num_threads for _ in range(self.num_threads)]
        snapshots: list[list[VectorClock]] = [[] for _ in range(self.num_threads)]
        periods = self._tick_periods()
        sync = self._sync_handler()
        # Steps are the global schedule, not the workers' progress: delivering
        # messages in step order stays causally consistent however slowly an
        # individual worker advances its own component.
        for step in range(1, steps + 1):
            for thread, period in enumerate(periods):
                if step % period == 0:
                    clocks[thread][thread] += 1
            if sync is not None:
                sync(clocks, step, rng)
            for thread, clock in enumerate(clocks):
                snapshots[thread].append(tuple(clock))
        return snapshots

    def _sync_handler(
        self,
    ) -> Callable[[list[list[int]], int, random.Random], None] | None:
        """The pattern's step handler, resolved once per simulation."""
        if self.num_threads < 2 or self.pattern == SyncPattern.INDEPENDENT:
            return None
        return getattr(self, f"_sync_{self.pattern}")

    def _sync_pairs(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        if step % self.sync_period == 0:
            for lo in range(0, self.num_threads - 1, 2):
                _merge(clocks, range(lo, lo + 2))

    def _sync_subset(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        if step % self.sync_period == 0:
            _merge(clocks, range(self.num_threads // 2 or 1))

    def _sync_sparse(
        self, clocks: list[list[int]], step: int, rng: random.Random
    ) -> None:
        if step % self.sync_period == 0:
            sender, receiver = rng.sample(range(self.num_threads), 2)
            _deliver(clocks, sender, receiver)

    def _sync_irregular(
        self, clocks: list[list[int]], _step: int, rng: random.Random
    ) -> None:
        """Aperiodic barrier over a random subset: no period to align with."""
        if rng.random() >= 1.0 / self.sync_period:
            return
        size = rng.randint(2, self.num_threads)
        _merge(clocks, rng.sample(range(self.num_threads), size))

    def _sync_ring(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        sender = step % self.num_threads
        _deliver(clocks, sender, (sender + 1) % self.num_threads)

    def _sync_tree(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        """Butterfly reduction: stride doubles each period, log2 levels deep."""
        if step % self.sync_period != 0:
            return
        levels = max(1, (self.num_threads - 1).bit_length())
        stride = 1 << ((step // self.sync_period - 1) % levels)
        for lo in range(0, self.num_threads, 2 * stride):
            if lo + stride < self.num_threads:
                _merge(clocks, range(lo, min(lo + 2 * stride, self.num_threads)))

    def _sync_barrier(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        if step % self.sync_period == 0:
            _merge(clocks, range(self.num_threads))

    def _sync_dense(
        self, clocks: list[list[int]], _step: int, rng: random.Random
    ) -> None:
        for receiver in range(self.num_threads):
            sender = receiver + rng.randrange(1, self.num_threads)
            _deliver(clocks, sender % self.num_threads, receiver)

    def _sync_groups(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        if step % (4 * self.sync_period) == 0:
            _merge(clocks, range(self.num_threads))
        elif step % self.sync_period == 0:
            group_size = self.group_size or max(2, isqrt(self.num_threads))
            for lo in range(0, self.num_threads, group_size):
                hi = min(lo + group_size, self.num_threads)
                _merge(clocks, range(lo, hi))

    def _sync_fork_join(
        self, clocks: list[list[int]], step: int, _rng: random.Random
    ) -> None:
        phase = step % self.sync_period
        # With sync_period == 1 every phase is 0, so fork and join coincide
        fork_phase = 1 if self.sync_period > 1 else 0
        if phase == fork_phase:
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


def _merge(clocks: list[list[int]], group: Sequence[int]) -> None:
    """Barrier: every thread in the group adopts the group's join."""
    joined = [max(values) for values in zip(*(clocks[t] for t in group), strict=True)]
    for thread in group:
        clocks[thread][:] = joined
