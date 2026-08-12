#
# SPDX-License-Identifier: Apache-2.0
#

import random
from typing import ClassVar

from omnimalloc.common.constants import DEFAULT_SEED, KB, MB
from omnimalloc.primitives import Allocation

from .base import BaseSource
from .sizes import SizeDistribution, sample_sizes


class SkewedSource(BaseSource):
    """Scalar allocations whose sizes come from a heavy-tailed distribution.

    The shipped generators draw sizes flat, the one regime where sorting by size
    is uninformative, while real scopes are skewed. `dominant` is the extreme.
    """

    _label_fields: ClassVar[tuple[str, ...]] = (
        "distribution",
        "size_min",
        "size_max",
        "time_max",
        "duration_min",
        "duration_max",
        "seed",
    )

    def __init__(
        self,
        num_allocations: int = 128,
        distribution: SizeDistribution | str = SizeDistribution.DOMINANT,
        size_min: int = KB,
        size_max: int = MB,
        time_max: int = 1024,
        duration_min: int = 1,
        duration_max: int = 64,
        seed: int | None = DEFAULT_SEED,
    ) -> None:
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if size_max < size_min:
            raise ValueError("size_max must be >= size_min")
        if duration_min <= 0:
            raise ValueError("duration_min must be positive")
        if duration_max < duration_min:
            raise ValueError("duration_max must be >= duration_min")
        if time_max <= duration_max:
            raise ValueError("time_max must be > duration_max")
        super().__init__(num_allocations=num_allocations)
        self.distribution = SizeDistribution(distribution)
        self.size_min = size_min
        self.size_max = size_max
        self.time_max = time_max
        self.duration_min = duration_min
        self.duration_max = duration_max
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        num = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(None if self.seed is None else self.seed + skip)
        sizes = sample_sizes(rng, num, self.distribution, self.size_min, self.size_max)

        allocations = []
        for i, size in enumerate(sizes):
            duration = rng.randint(self.duration_min, self.duration_max)
            start = rng.randint(0, self.time_max - duration)
            allocations.append(
                Allocation(id=skip + i, size=size, start=start, end=start + duration)
            )
        return tuple(allocations)


class TwoPlusTwoSource(BaseSource):
    """Vector-clock instances that provably are not interval orders.

    Every group of four allocations is a 2+2, inducing a chordless 4-cycle no set
    of intervals can realize, so `try_linearize` always returns None here.
    """

    _GROUP = 4
    _label_fields: ClassVar[tuple[str, ...]] = ("noise", "size_min", "size_max", "seed")

    def __init__(
        self,
        num_allocations: int = 128,
        noise: float = 0.0,
        size_min: int = KB,
        size_max: int = MB,
        seed: int | None = DEFAULT_SEED,
    ) -> None:
        if not 0.0 <= noise < 1.0:
            raise ValueError("noise must be in [0, 1)")
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if size_max < size_min:
            raise ValueError("size_max must be >= size_min")
        super().__init__(num_allocations=num_allocations)
        self.noise = noise
        self.size_min = size_min
        self.size_max = size_max
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        num = num_allocations if num_allocations is not None else self.num_allocations
        if num < self._GROUP:
            raise ValueError(
                f"TwoPlusTwoSource needs at least {self._GROUP} allocations, got {num}"
            )
        rng = random.Random(None if self.seed is None else self.seed + skip)
        obstructions = max(1, round(num * (1.0 - self.noise)) // self._GROUP)

        allocations = []
        for group in range(obstructions):
            base = self._GROUP * group
            # One pair advances on lane 0, the other on lane 1, so neither
            # pair's end dominates the other pair's start
            for start, end in (
                ((base, base), (base + 1, base)),
                ((base + 1, base), (base + 2, base)),
                ((base, base), (base, base + 1)),
                ((base, base + 1), (base, base + 2)),
            ):
                allocations.append(
                    self._allocation(rng, skip + len(allocations), start, end)
                )

        while len(allocations) < num:
            lane = rng.randrange(2)
            step = rng.randrange(self._GROUP * obstructions or 1)
            start = (step, 0) if lane == 0 else (0, step)
            end = (step + 1, 0) if lane == 0 else (0, step + 1)
            allocations.append(
                self._allocation(rng, skip + len(allocations), start, end)
            )

        return tuple(allocations[:num])

    def _allocation(
        self,
        rng: random.Random,
        index: int,
        start: tuple[int, int],
        end: tuple[int, int],
    ) -> Allocation:
        return Allocation(
            id=index,
            size=rng.randint(self.size_min, self.size_max),
            start=start,
            end=end,
        )
