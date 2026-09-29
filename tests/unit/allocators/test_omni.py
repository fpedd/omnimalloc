#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc.allocators import OmniAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.primitives import Allocation

# Linearization matches the greedy portfolio here and beats it below
MATCHED = (
    Allocation(id=0, size=30, start=(0, 2), end=(0, 3)),
    Allocation(id=1, size=2, start=(0, 2), end=(0, 3)),
    Allocation(id=2, size=19, start=(0, 1), end=(2, 1)),
    Allocation(id=3, size=6, start=(4, 0), end=(6, 2)),
    Allocation(id=4, size=23, start=(4, 4), end=(5, 6)),
    Allocation(id=5, size=9, start=(1, 1), end=(1, 3)),
    Allocation(id=6, size=24, start=(0, 3), end=(2, 3)),
    Allocation(id=7, size=11, start=(2, 3), end=(3, 3)),
    Allocation(id=8, size=26, start=(4, 0), end=(4, 1)),
    Allocation(id=9, size=26, start=(2, 4), end=(4, 6)),
    Allocation(id=10, size=29, start=(1, 4), end=(3, 5)),
)
WIDENED = (
    Allocation(id=0, size=1, start=(6, 12), end=(9, 15)),
    Allocation(id=1, size=2, start=(16, 7), end=(24, 11)),
    Allocation(id=2, size=16, start=(17, 8), end=(20, 14)),
    Allocation(id=3, size=16, start=(8, 2), end=(14, 10)),
    Allocation(id=4, size=4, start=(0, 4), end=(7, 12)),
    Allocation(id=5, size=32, start=(12, 19), end=(13, 25)),
    Allocation(id=6, size=2, start=(5, 9), end=(12, 17)),
    Allocation(id=7, size=8, start=(9, 17), end=(14, 23)),
    Allocation(id=8, size=1, start=(16, 4), end=(22, 9)),
    Allocation(id=9, size=16, start=(6, 12), end=(9, 19)),
    Allocation(id=10, size=8, start=(11, 20), end=(15, 24)),
    Allocation(id=11, size=8, start=(17, 9), end=(20, 10)),
    Allocation(id=12, size=16, start=(19, 9), end=(25, 12)),
    Allocation(id=13, size=32, start=(19, 7), end=(24, 11)),
    Allocation(id=14, size=16, start=(3, 7), end=(9, 9)),
)


def _peak(allocations: tuple[Allocation, ...], linearize_budget: int | None) -> int:
    placed = OmniAllocator(linearize_budget=linearize_budget).allocate(allocations)
    return placement_pressure(placed)


def _random_vector(n: int, dim: int, seed: int) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(n):
        start = tuple(rng.randint(0, 6) for _ in range(dim))
        delta = [rng.randint(0, 2) for _ in range(dim)]
        delta[rng.randrange(dim)] += 1
        end = tuple(s + d for s, d in zip(start, delta, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 64), start=start, end=end)
        )
    return tuple(allocations)


def test_omni_rejects_negative_linearize_budget() -> None:
    with pytest.raises(ValueError, match="linearize_budget"):
        OmniAllocator(linearize_budget=-1)


@pytest.mark.parametrize(
    ("allocations", "linearize_budget", "greedy_peak", "peak"),
    [(MATCHED, None, 104, 104), (WIDENED, DEFAULT_WORK_BUDGET, 140, 139)],
)
def test_omni_linearization_never_loses_to_the_greedy_portfolio(
    allocations: tuple[Allocation, ...],
    linearize_budget: int | None,
    greedy_peak: int,
    peak: int,
) -> None:
    assert _peak(allocations, linearize_budget=0) == greedy_peak
    assert _peak(allocations, linearize_budget) == peak


@pytest.mark.parametrize("dim", [2, 3, 4])
def test_omni_linearize_budget_is_quality_monotone(dim: int) -> None:
    for seed in range(10):
        allocations = _random_vector(24, dim=dim, seed=1000 * dim + seed)
        floor = _peak(allocations, linearize_budget=0)
        assert _peak(allocations, DEFAULT_WORK_BUDGET) <= floor
        assert _peak(allocations, linearize_budget=None) <= floor


def test_omni_degenerate_clock_columns_match_the_scalar_instance() -> None:
    rng = random.Random(31)
    for _ in range(60):
        base = [
            (i, rng.randint(1, 50), rng.randint(0, 60), rng.randint(1, 12))
            for i in range(rng.randint(1, 40))
        ]
        scalar = tuple(
            Allocation(id=i, size=z, start=s, end=s + d) for i, z, s, d in base
        )
        padded = tuple(
            Allocation(id=i, size=z, start=(s, 0, 0), end=(s + d, 0, 0))
            for i, z, s, d in base
        )
        lockstep = tuple(
            Allocation(id=i, size=z, start=(s,) * 3, end=(s + d,) * 3)
            for i, z, s, d in base
        )
        expected = _peak(scalar, DEFAULT_WORK_BUDGET)
        assert _peak(padded, DEFAULT_WORK_BUDGET) == expected
        assert _peak(lockstep, DEFAULT_WORK_BUDGET) == expected
