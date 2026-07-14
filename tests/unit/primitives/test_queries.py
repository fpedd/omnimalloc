#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.queries import (
    GreedyOrder,
    Guarantee,
    get_conflicts,
    get_per_allocation_pressure,
    get_pressure,
)
from omnimalloc.primitives.vector_clock import happens_before, time_components

W = 10


def non_helly_triple() -> tuple[Allocation, ...]:
    return (
        Allocation(id="a", size=W, start=(0, 0), end=(2, 2)),
        Allocation(id="b", size=W, start=(3, 0), end=(4, 1)),
        Allocation(id="c", size=W, start=(0, 3), end=(1, 4)),
    )


def porcupine(n: int = 4) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=i,
            size=W,
            start=tuple(0 if k == i else 1 for k in range(n)),
            end=(1,) * n,
        )
        for i in range(n)
    )


def random_vector_allocations(rng: random.Random) -> tuple[Allocation, ...]:
    dim = rng.randint(2, 3)
    allocations = []
    for i in range(rng.randint(1, 8)):
        start = tuple(rng.randint(0, 4) for _ in range(dim))
        deltas = [rng.randint(0, 3) for _ in range(dim)]
        deltas[rng.randrange(dim)] += 1
        end = tuple(s + d for s, d in zip(start, deltas, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 9), start=start, end=end)
        )
    return tuple(allocations)


def brute_peaks(allocations: tuple[Allocation, ...]) -> dict[int | str, int]:
    births = [time_components(alloc.start) for alloc in allocations]
    deaths = [time_components(alloc.end) for alloc in allocations]
    peaks = {alloc.id: alloc.size for alloc in allocations}
    for mask in range(1, 1 << len(allocations)):
        members = [i for i in range(len(allocations)) if mask >> i & 1]
        cut = tuple(max(c) for c in zip(*(births[i] for i in members), strict=True))
        resident = all(
            happens_before(births[i], cut) and not happens_before(deaths[i], cut)
            for i in members
        )
        if resident:
            weight = sum(allocations[i].size for i in members)
            for i in members:
                peaks[allocations[i].id] = max(peaks[allocations[i].id], weight)
    return peaks


def test_pressure_scalar_sweep() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=4, end=8),
    )
    assert get_pressure(allocations) == 150


def test_pressure_empty() -> None:
    assert get_pressure(()) == 0


def test_pressure_vector_concurrent_pair_sums() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=100, start=(1, 0), end=(3, 1)),
    )
    assert get_pressure(allocations) == 200


def test_pressure_vector_ordered_pair_takes_max() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=50, start=(2, 1), end=(3, 2)),
    )
    assert get_pressure(allocations) == 100


def test_pressure_vector_chain_takes_max() -> None:
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=(i, i), end=(i + 1, i + 1))
        for i in range(5)
    )
    assert get_pressure(allocations) == 50


def test_pressure_vector_conflict_without_lane_overlap() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 5), end=(1, 6)),
        Allocation(id=2, size=100, start=(2, 0), end=(3, 1)),
    )
    assert get_pressure(allocations) == 200


def test_pressure_vector_unsynced_threads_sum() -> None:
    allocations = (
        Allocation(id=1, size=64, start=(3, 0), end=(5, 0)),
        Allocation(id=2, size=32, start=(0, 2), end=(0, 4)),
    )
    assert get_pressure(allocations) == 96


def test_queries_mixed_dimensions_rejected() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="clock dim"):
        get_pressure(allocations)
    with pytest.raises(ValueError, match="clock dim"):
        get_conflicts(allocations)
    with pytest.raises(ValueError, match="clock dim"):
        get_per_allocation_pressure(allocations)


def test_conflicts_scalar_chain() -> None:
    allocations = (
        Allocation(id=1, size=1, start=0, end=4),
        Allocation(id=2, size=1, start=2, end=6),
        Allocation(id=3, size=1, start=4, end=8),
    )
    assert get_conflicts(allocations) == {1: {2}, 2: {1, 3}, 3: {2}}


def test_conflicts_isolated_allocations_have_empty_sets() -> None:
    allocations = (
        Allocation(id=1, size=1, start=0, end=2),
        Allocation(id=2, size=1, start=2, end=4),
    )
    assert get_conflicts(allocations) == {1: set(), 2: set()}


def test_conflicts_empty() -> None:
    assert get_conflicts(()) == {}


def test_conflicts_duplicate_ids_rejected() -> None:
    allocations = (
        Allocation(id=1, size=1, start=0, end=2),
        Allocation(id=1, size=1, start=1, end=3),
    )
    with pytest.raises(ValueError, match="unique"):
        get_conflicts(allocations)
    with pytest.raises(ValueError, match="unique"):
        get_per_allocation_pressure(allocations)


def test_conflicting_pair_never_coresident() -> None:
    allocations = (
        Allocation(id="a", size=3, start=(0, 5), end=(2, 5)),
        Allocation(id="b", size=5, start=(5, 0), end=(5, 3)),
    )
    assert get_conflicts(allocations) == {"a": {"b"}, "b": {"a"}}
    assert get_pressure(allocations, Guarantee.ANTICHAIN) == 8
    assert get_pressure(allocations, Guarantee.EXACT) == 5
    assert get_per_allocation_pressure(allocations, Guarantee.EXACT) == {
        "a": 3,
        "b": 5,
    }


def test_per_allocation_pressure_empty() -> None:
    assert get_per_allocation_pressure(()) == {}


def test_per_allocation_pressure_single() -> None:
    allocations = (Allocation(id="x", size=7, start=(0, 0), end=(1, 1)),)
    assert get_per_allocation_pressure(allocations) == {"x": 7}


def test_per_allocation_pressure_scalar_sweep() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=4, end=8),
    )
    assert get_per_allocation_pressure(allocations) == {1: 150, 2: 150, 3: 75}


def test_per_allocation_pressure_non_helly_triple() -> None:
    triple = non_helly_triple()
    assert get_pressure(triple, Guarantee.ANTICHAIN) == 3 * W
    assert get_pressure(triple, Guarantee.EXACT) == 2 * W
    assert get_per_allocation_pressure(triple, Guarantee.EXACT) == {
        "a": 2 * W,
        "b": 2 * W,
        "c": 2 * W,
    }


def test_per_allocation_pressure_porcupine() -> None:
    allocations = porcupine()
    assert get_pressure(allocations, Guarantee.ANTICHAIN) == 4 * W
    assert get_pressure(allocations, Guarantee.EXACT) == W
    assert get_per_allocation_pressure(allocations, Guarantee.EXACT) == dict.fromkeys(
        range(4), W
    )


def test_per_allocation_pressure_antichain_when_capped() -> None:
    peaks = get_per_allocation_pressure(porcupine(), Guarantee.ANTICHAIN, closure_cap=2)
    assert peaks == dict.fromkeys(range(4), 4 * W)


def test_per_allocation_pressure_exact_raises_when_uncertified() -> None:
    with pytest.raises(RuntimeError, match="unresolved"):
        get_per_allocation_pressure(porcupine(), Guarantee.EXACT, closure_cap=2)


def test_per_allocation_pressure_bound_runs_greedy_placement() -> None:
    allocations = (
        Allocation(id="a", size=W, start=(0, 0), end=(1, 1)),
        Allocation(id="b", size=W, start=(0, 0), end=(2, 2)),
        Allocation(id="c", size=W, start=(1, 1), end=(2, 2)),
    )
    peaks = get_per_allocation_pressure(allocations, Guarantee.BOUND)
    assert peaks == {"a": 2 * W, "b": 2 * W, "c": 2 * W}


@pytest.mark.parametrize("placer", list(GreedyOrder))
def test_per_allocation_pressure_bound_placer_selectable(placer: GreedyOrder) -> None:
    allocations = (
        Allocation(id="a", size=W, start=(0, 0), end=(1, 1)),
        Allocation(id="b", size=W, start=(0, 0), end=(2, 2)),
        Allocation(id="c", size=W, start=(1, 1), end=(2, 2)),
    )
    peaks = get_per_allocation_pressure(allocations, Guarantee.BOUND, placer=placer)
    assert peaks == {"a": 2 * W, "b": 2 * W, "c": 2 * W}


def test_per_allocation_pressure_bound_ignores_offsets() -> None:
    unplaced = (
        Allocation(id="a", size=W, start=(0, 0), end=(1, 1)),
        Allocation(id="b", size=W, start=(0, 0), end=(2, 2)),
        Allocation(id="c", size=W, start=(1, 1), end=(2, 2)),
    )
    placed = tuple(
        Allocation(id=a.id, size=a.size, start=a.start, end=a.end, offset=i * 100)
        for i, a in enumerate(unplaced)
    )
    bound = get_per_allocation_pressure(unplaced, Guarantee.BOUND)
    assert get_per_allocation_pressure(placed, Guarantee.BOUND) == bound


def test_per_allocation_pressure_scalar_max_matches_global() -> None:
    rng = random.Random(3)
    for _ in range(20):
        allocations = []
        for i in range(rng.randint(1, 30)):
            start = rng.randint(0, 20)
            end = start + rng.randint(1, 10)
            allocations.append(
                Allocation(id=i, size=rng.randint(1, 100), start=start, end=end)
            )
        scalar = tuple(allocations)
        peaks = get_per_allocation_pressure(scalar)
        assert max(peaks.values()) == get_pressure(scalar)
        lockstep = tuple(
            Allocation(
                id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end)
            )
            for a in scalar
        )
        assert get_per_allocation_pressure(lockstep, Guarantee.EXACT) == peaks
        assert get_pressure(lockstep, Guarantee.EXACT) == get_pressure(scalar)


def test_per_allocation_pressure_guarantees_are_monotone() -> None:
    rng = random.Random(11)
    for _ in range(20):
        allocations = random_vector_allocations(rng)
        exact = get_per_allocation_pressure(allocations, Guarantee.EXACT)
        antichain = get_per_allocation_pressure(
            allocations, Guarantee.ANTICHAIN, closure_cap=0
        )
        bound = get_per_allocation_pressure(allocations, Guarantee.BOUND)
        for key in exact:
            assert exact[key] <= antichain[key] <= bound[key]


def test_pressure_default_is_bound() -> None:
    triple = non_helly_triple()
    assert get_pressure(triple) == get_pressure(triple, Guarantee.BOUND)
    assert get_pressure(triple) == max(get_per_allocation_pressure(triple).values())


def test_pressure_bound_ignores_existing_offsets() -> None:
    allocations = (
        Allocation(id="a", size=W, start=(0, 0), end=(1, 1), offset=0),
        Allocation(id="b", size=W, start=(0, 0), end=(2, 2), offset=W),
        Allocation(id="c", size=W, start=(1, 1), end=(2, 2), offset=100),
    )
    assert get_pressure(allocations) == 2 * W


def test_pressure_guarantees_are_monotone() -> None:
    rng = random.Random(23)
    for _ in range(20):
        allocations = random_vector_allocations(rng)
        exact = get_pressure(allocations, Guarantee.EXACT)
        antichain = get_pressure(allocations, Guarantee.ANTICHAIN)
        bound = get_pressure(allocations, Guarantee.BOUND)
        assert exact <= antichain <= bound


def test_pressure_exact_raises_when_uncertified() -> None:
    with pytest.raises(RuntimeError, match="unresolved"):
        get_pressure(porcupine(), Guarantee.EXACT, closure_cap=2)


def test_per_allocation_pressure_matches_brute_force() -> None:
    rng = random.Random(7)
    for _ in range(30):
        allocations = random_vector_allocations(rng)
        peaks = get_per_allocation_pressure(allocations, Guarantee.EXACT)
        assert peaks == brute_peaks(allocations)
