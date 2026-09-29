#
# SPDX-License-Identifier: Apache-2.0
#
"""Metamorphic properties of the analysis and of the default omni allocator."""

import itertools
import random

import pytest
from omnimalloc import Allocation, allocate, validate_allocation
from omnimalloc.analysis import (
    antichain_pressure,
    closure_pressure,
    conflict_degrees,
    conflicts,
    placement_pressure,
    try_linearize,
)

SEEDS = (0, 1, 2, 3)

LANE_SENSITIVE = (
    Allocation(id=0, size=2, start=(0, 0), end=(1, 2)),
    Allocation(id=1, size=8, start=(1, 0), end=(2, 1)),
    Allocation(id=2, size=5, start=(3, 1), end=(5, 3)),
    Allocation(id=3, size=9, start=(2, 2), end=(4, 3)),
    Allocation(id=4, size=4, start=(1, 0), end=(3, 0)),
)


def _scalar_instance(
    seed: int, count: int = 40, horizon: int = 30
) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(count):
        start = rng.randint(0, horizon)
        size, duration = rng.randint(1, 512), rng.randint(1, 8)
        allocations.append(
            Allocation(id=i, size=size, start=start, end=start + duration)
        )
    return tuple(allocations)


def _vector_instance(seed: int, count: int = 24) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(count):
        start = tuple(rng.randint(0, 6) for _ in range(3))
        delta = [rng.randint(0, 3) for _ in range(3)]
        delta[rng.randrange(3)] += 1
        end = tuple(s + d for s, d in zip(start, delta, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 256), start=start, end=end)
        )
    return tuple(allocations)


INSTANCES = [
    *(pytest.param(_scalar_instance(seed), id=f"scalar{seed}") for seed in SEEDS),
    *(pytest.param(_vector_instance(seed), id=f"vector{seed}") for seed in SEEDS),
]
VECTOR_INSTANCES = INSTANCES[len(SEEDS) :]


def _peak(allocations: tuple[Allocation, ...]) -> int:
    return placement_pressure(allocate(allocations, "omni"))


def _antichain(allocations: tuple[Allocation, ...]) -> int:
    return antichain_pressure(allocations, work_budget=None)


def _closure(allocations: tuple[Allocation, ...]) -> int:
    return closure_pressure(allocations, closure_cap=None)


def _map(
    allocations: tuple[Allocation, ...], size: int = 1, clock: object = None
) -> tuple[Allocation, ...]:
    """Scale every size by `size` and send every clock through `clock`."""
    return tuple(
        Allocation(
            id=a.id,
            size=size * a.size,
            start=clock(a.start) if clock else a.start,
            end=clock(a.end) if clock else a.end,
        )
        for a in allocations
    )


def _optimum(allocations: tuple[Allocation, ...]) -> int:
    return min(
        placement_pressure(allocate([allocations[i] for i in order], "greedy"))
        for order in itertools.permutations(range(len(allocations)))
    )


@pytest.mark.parametrize("allocations", INSTANCES)
def test_the_closure_bound_stays_under_the_antichain_bound_and_the_peak(
    allocations: tuple[Allocation, ...],
) -> None:
    assert _closure(allocations) <= _antichain(allocations) <= _peak(allocations)


@pytest.mark.parametrize("allocations", INSTANCES)
@pytest.mark.parametrize("factor", [3, 11])
def test_uniform_size_scaling_scales_the_pressures_and_the_peak(
    allocations: tuple[Allocation, ...], factor: int
) -> None:
    scaled = _map(allocations, size=factor)
    assert _antichain(scaled) == factor * _antichain(allocations)
    assert _closure(scaled) == factor * _closure(allocations)
    assert _peak(scaled) == factor * _peak(allocations)


@pytest.mark.parametrize("allocations", VECTOR_INSTANCES)
@pytest.mark.parametrize(
    "clock",
    [
        pytest.param(lambda t: (*t, 0), id="zero_thread_padding"),
        pytest.param(lambda t: tuple(7 * x for x in t), id="time_dilation"),
        pytest.param(lambda t: (t[2], t[0], t[1]), id="thread_permutation"),
        pytest.param(lambda t: (3 * t[1], 3 * t[0], 0, 3 * t[2]), id="composed"),
    ],
)
def test_order_preserving_clock_maps_keep_conflicts_pressures_and_peak(
    allocations: tuple[Allocation, ...], clock: object
) -> None:
    mapped = _map(allocations, clock=clock)
    assert conflicts(mapped, None) == conflicts(allocations, None)
    assert conflict_degrees(mapped, None) == conflict_degrees(allocations, None)
    assert _antichain(mapped) == _antichain(allocations)
    assert _closure(mapped) == _closure(allocations)
    assert _peak(mapped) == _peak(allocations)


@pytest.mark.parametrize("allocations", VECTOR_INSTANCES)
def test_every_thread_relabelling_keeps_the_omni_peak(
    allocations: tuple[Allocation, ...],
) -> None:
    peak = _peak(allocations)
    for lanes in itertools.permutations(range(3)):
        permuted = _map(
            allocations, clock=lambda t, lanes=lanes: tuple(t[i] for i in lanes)
        )
        assert _peak(permuted) == peak


def test_thread_permutation_keeps_the_peak_of_a_lane_sensitive_instance() -> None:
    swapped = _map(LANE_SENSITIVE, clock=lambda t: t[::-1])
    assert conflicts(swapped, None) == conflicts(LANE_SENSITIVE, None)
    placed = allocate(swapped, "omni")
    validate_allocation(placed)
    assert placement_pressure(placed) == _peak(LANE_SENSITIVE)


def test_removing_an_allocation_cannot_raise_the_optimum() -> None:
    rng = random.Random(11)
    for _ in range(5):
        allocations = _scalar_instance(rng.randrange(1000), count=6, horizon=6)
        optimum = _optimum(allocations)
        for i in range(len(allocations)):
            assert _optimum(allocations[:i] + allocations[i + 1 :]) <= optimum


@pytest.mark.parametrize("seed", SEEDS)
def test_removing_an_allocation_cannot_raise_the_bound(seed: int) -> None:
    allocations = _scalar_instance(seed, count=20)
    bound = _antichain(allocations)
    for i in range(len(allocations)):
        assert _antichain(allocations[:i] + allocations[i + 1 :]) <= bound


def test_linearizing_a_staircase_keeps_its_conflicts_and_pressure() -> None:
    allocations = tuple(
        Allocation(id=i, size=8 + i, start=(i, 2 * i), end=(i + 2, 2 * i + 3))
        for i in range(30)
    )
    linearized = try_linearize(allocations, work_budget=None)
    assert linearized is not None
    assert all(alloc.dim == 1 for alloc in linearized)
    assert conflicts(linearized, None) == conflicts(allocations, None)
    assert _antichain(tuple(linearized)) == _antichain(allocations)
