#
# SPDX-License-Identifier: Apache-2.0
#

import itertools
import random

from omnimalloc.primitives import Allocation
from omnimalloc.primitives.vector_clock import (
    happens_before,
    time_components,
    vector_pressure,
)


def brute_max_weight_antichain(allocations: tuple[Allocation, ...]) -> int:
    ends = [time_components(a.end) for a in allocations]
    starts = [time_components(a.start) for a in allocations]

    def ordered(i: int, j: int) -> bool:
        return happens_before(ends[i], starts[j]) or happens_before(ends[j], starts[i])

    indices = range(len(allocations))
    best = 0
    for size in indices:
        for subset in itertools.combinations(indices, size + 1):
            if all(not ordered(i, j) for i, j in itertools.combinations(subset, 2)):
                best = max(best, sum(allocations[i].size for i in subset))
    return best


def test_vector_pressure_single_allocation() -> None:
    allocations = (Allocation(id=1, size=42, start=(0, 0), end=(1, 1)),)
    assert vector_pressure(allocations) == 42


def test_vector_pressure_concurrent_pair_sums() -> None:
    allocations = (
        Allocation(id=1, size=30, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=70, start=(1, 0), end=(3, 1)),
    )
    assert vector_pressure(allocations) == 100


def test_vector_pressure_ordered_pair_takes_max() -> None:
    allocations = (
        Allocation(id=1, size=30, start=(0, 0), end=(1, 1)),
        Allocation(id=2, size=70, start=(1, 1), end=(2, 2)),
    )
    assert vector_pressure(allocations) == 70


def test_vector_pressure_maximizes_weight_not_cardinality() -> None:
    allocations = (
        Allocation(id="y", size=10, start=(0, 0), end=(1, 1)),
        Allocation(id="z", size=10, start=(0, 1), end=(1, 2)),
        Allocation(id="x", size=100, start=(1, 2), end=(2, 3)),
    )
    assert vector_pressure(allocations) == 100


def test_vector_pressure_matches_brute_force() -> None:
    rng = random.Random(7)
    for _ in range(200):
        allocations = []
        for i in range(rng.randint(1, 7)):
            dim = 3
            start = tuple(rng.randint(0, 4) for _ in range(dim))
            deltas = [rng.randint(0, 3) for _ in range(dim)]
            if sum(deltas) == 0:
                deltas[rng.randrange(dim)] = 1
            end = tuple(s + d for s, d in zip(start, deltas, strict=True))
            allocations.append(
                Allocation(id=i, size=rng.randint(1, 9), start=start, end=end)
            )
        allocations = tuple(allocations)
        assert vector_pressure(allocations) == brute_max_weight_antichain(allocations)
