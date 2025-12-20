#
# SPDX-License-Identifier: Apache-2.0
#

import random

from omnimalloc.primitives import Allocation, BufferKind

from .base import BaseSource


class RandomSource(BaseSource):
    """Generate random allocations with random sizes, starts, and durations."""

    def __init__(
        self,
        num_allocations: int = 100,
        size_min: int = 1024,
        size_max: int = 1024 * 1024,
        time_min: int = 0,
        time_max: int = 10000,
        duration_min: int = 1,
        duration_max: int = 500,
        kinds: tuple[BufferKind, ...] | None = None,
        kind_weights: tuple[float, ...] | None = None,
        seed: int | None = 42,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if size_max < size_min:
            raise ValueError("size_max must be >= size_min")
        if time_min < 0:
            raise ValueError("time_min must be non-negative")
        if time_max <= time_min:
            raise ValueError("time_max must be > time_min")
        if duration_min <= 0:
            raise ValueError("duration_min must be positive")
        if duration_max < duration_min:
            raise ValueError("duration_max must be >= duration_min")
        if duration_max > (time_max - time_min):
            raise ValueError("duration_max must fit within time bounds")
        if kinds and kind_weights and len(kinds) != len(kind_weights):
            raise ValueError("kinds and kind_weights must have same length")

        self.size_min = size_min
        self.size_max = size_max
        self.time_min = time_min
        self.time_max = time_max
        self.duration_min = duration_min
        self.duration_max = duration_max
        self.kinds = kinds
        self.kind_weights = kind_weights
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        total = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(self.seed)

        for _ in range(skip):
            self._generate_one(rng, 0)

        return tuple(self._generate_one(rng, skip + i) for i in range(total))

    def _generate_one(self, rng: random.Random, alloc_id: int) -> Allocation:
        size = rng.randint(self.size_min, self.size_max)
        duration = rng.randint(self.duration_min, self.duration_max)
        max_start = self.time_max - duration
        start = rng.randint(self.time_min, max(self.time_min, max_start))

        kind = None
        if self.kinds:
            kind = rng.choices(self.kinds, weights=self.kind_weights, k=1)[0]

        return Allocation(
            id=alloc_id,
            size=size,
            start=start,
            end=start + duration,
            kind=kind,
        )


class UniformSource(BaseSource):
    """Generate uniform-sized allocations with random start times."""

    def __init__(
        self,
        num_allocations: int = 100,
        size: int = 4096,
        duration: int = 10,
        time_max: int = 100,
        seed: int | None = 42,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if size <= 0:
            raise ValueError("size must be positive")
        if duration <= 0:
            raise ValueError("duration must be positive")
        if time_max <= 0:
            raise ValueError("time_max must be positive")
        if duration > time_max:
            raise ValueError("duration must be <= time_max")

        self.size = size
        self.duration = duration
        self.time_max = time_max
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        total = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(self.seed)

        for _ in range(skip):
            self._generate_one(rng, 0)

        return tuple(self._generate_one(rng, skip + i) for i in range(total))

    def _generate_one(self, rng: random.Random, alloc_id: int) -> Allocation:
        max_start = max(0, self.time_max - self.duration)
        start = rng.randint(0, max_start)

        return Allocation(
            id=alloc_id,
            size=self.size,
            start=start,
            end=start + self.duration,
        )


class PowerOf2Source(BaseSource):
    """Generate allocations with power-of-2 sizes."""

    def __init__(
        self,
        num_allocations: int = 100,
        size_exponent_min: int = 10,
        size_exponent_max: int = 20,
        time_max: int = 100,
        duration_min: int = 1,
        duration_max: int = 50,
        seed: int | None = 42,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if size_exponent_min < 0:
            raise ValueError("size_exponent_min must be non-negative")
        if size_exponent_max < size_exponent_min:
            raise ValueError("size_exponent_max must be >= size_exponent_min")
        if time_max <= 0:
            raise ValueError("time_max must be positive")
        if duration_min <= 0:
            raise ValueError("duration_min must be positive")
        if duration_max < duration_min:
            raise ValueError("duration_max must be >= duration_min")

        self.size_exponent_min = size_exponent_min
        self.size_exponent_max = size_exponent_max
        self.time_max = time_max
        self.duration_min = duration_min
        self.duration_max = duration_max
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        total = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(self.seed)

        for _ in range(skip):
            self._generate_one(rng, 0)

        return tuple(self._generate_one(rng, skip + i) for i in range(total))

    def _generate_one(self, rng: random.Random, alloc_id: int) -> Allocation:
        exponent = rng.randint(self.size_exponent_min, self.size_exponent_max)
        duration = rng.randint(self.duration_min, self.duration_max)
        start = rng.randint(0, max(0, self.time_max - duration))

        return Allocation(
            id=alloc_id,
            size=2**exponent,
            start=start,
            end=start + duration,
        )


class HighContentionSource(BaseSource):
    """Generate allocations with high temporal contention in a small time window."""

    def __init__(
        self,
        num_allocations: int = 100,
        size_min: int = 1024,
        size_max: int = 1024 * 1024,
        time_window: int = 20,
        seed: int | None = 42,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if size_max < size_min:
            raise ValueError("size_max must be >= size_min")
        if time_window <= 0:
            raise ValueError("time_window must be positive")

        self.size_min = size_min
        self.size_max = size_max
        self.time_window = time_window
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        total = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(self.seed)

        for _ in range(skip):
            self._generate_one(rng, 0)

        return tuple(self._generate_one(rng, skip + i) for i in range(total))

    def _generate_one(self, rng: random.Random, alloc_id: int) -> Allocation:
        size = rng.randint(self.size_min, self.size_max)
        duration = rng.randint(1, self.time_window // 2)
        start = rng.randint(0, self.time_window - duration)

        return Allocation(
            id=alloc_id,
            size=size,
            start=start,
            end=start + duration,
        )


class SequentialSource(BaseSource):
    """Generate allocations with minimal temporal overlap."""

    def __init__(
        self,
        num_allocations: int = 100,
        size_min: int = 1024,
        size_max: int = 1024 * 1024,
        duration_min: int = 5,
        duration_max: int = 15,
        seed: int | None = 42,
    ) -> None:
        super().__init__(num_allocations=num_allocations)
        if size_min <= 0:
            raise ValueError("size_min must be positive")
        if size_max < size_min:
            raise ValueError("size_max must be >= size_min")
        if duration_min <= 0:
            raise ValueError("duration_min must be positive")
        if duration_max < duration_min:
            raise ValueError("duration_max must be >= duration_min")

        self.size_min = size_min
        self.size_max = size_max
        self.duration_min = duration_min
        self.duration_max = duration_max
        self.seed = seed

    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        total = num_allocations if num_allocations is not None else self.num_allocations
        rng = random.Random(self.seed)
        current_time = 0

        # Process skip allocations to maintain temporal continuity
        for _ in range(skip):
            _, current_time = self._generate_one(rng, 0, current_time)

        # Generate requested allocations
        allocations = []
        for i in range(total):
            alloc, current_time = self._generate_one(rng, skip + i, current_time)
            allocations.append(alloc)

        return tuple(allocations)

    def _generate_one(
        self, rng: random.Random, alloc_id: int, current_time: int
    ) -> tuple[Allocation, int]:
        """Generate one allocation and return it with the updated current_time."""
        size = rng.randint(self.size_min, self.size_max)
        duration = rng.randint(self.duration_min, self.duration_max)
        overlap = rng.randint(0, min(2, duration // 2))
        start = max(0, current_time - overlap)

        alloc = Allocation(
            id=alloc_id,
            size=size,
            start=start,
            end=start + duration,
        )

        return alloc, start + duration
