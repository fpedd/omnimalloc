#
# SPDX-License-Identifier: Apache-2.0
#

from itertools import combinations
from random import Random

import pytest
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.analysis.pressure import (
    get_closure_pressure,
    get_per_allocation_closure_pressure,
    get_per_allocation_placement_pressure,
    get_per_allocation_pressure,
    get_placement_pressure,
    get_pressure,
)
from omnimalloc.primitives import Allocation


def test_pressure_empty_is_zero() -> None:
    assert get_pressure(()) == 0


def test_pressure_scalar_overlap() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert get_pressure(allocations) == 150


def test_pressure_scalar_disjoint() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=2),
        Allocation(id=2, size=50, start=2, end=4),
    )
    assert get_pressure(allocations) == 100


def test_pressure_linearizable_vector_is_exact() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=50, start=(1, 0), end=(3, 2)),
        Allocation(id=3, size=25, start=(3, 2), end=(4, 3)),
    )
    assert get_pressure(allocations) == 150


def test_pressure_non_linearizable_is_exact() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert get_pressure(two_plus_two) == 16 + 64


def test_pressure_matches_scalar_equivalent_under_lockstep() -> None:
    scalar = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=5, end=8),
    )
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    assert get_pressure(lockstep) == get_pressure(scalar)


def test_pressure_scalar_ignores_work_budget() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
    )
    assert get_pressure(allocations, work_budget=1) == 150


def test_pressure_work_budget_exceeded_raises() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        get_pressure(two_plus_two, work_budget=1)


def test_pressure_negative_work_budget_rejected() -> None:
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        get_pressure((), work_budget=-1)


def test_closure_pressure_negative_cap_rejected() -> None:
    with pytest.raises(ValueError, match="closure_cap must be non-negative"):
        get_closure_pressure((), closure_cap=-1)


def test_pressure_total_size_overflow_raises() -> None:
    allocations = tuple(Allocation(id=i, size=2**62, start=0, end=1) for i in range(4))
    with pytest.raises(OverflowError, match="int64"):
        get_pressure(allocations)


def test_pressure_unbudgeted_empty_is_zero() -> None:
    assert get_pressure((), work_budget=None) == 0


def test_closure_pressure_empty_is_zero() -> None:
    assert get_closure_pressure(()) == 0


def test_exact_pressures_match_scalar_sweep() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert get_pressure(allocations, work_budget=None) == 150
    assert get_closure_pressure(allocations) == 150


def test_pressure_unbudgeted_two_plus_two_exact() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert get_pressure(two_plus_two, work_budget=None) == 16 + 64


def test_closure_pressure_below_antichain_without_common_cut() -> None:
    pinwheel = (
        Allocation(id="i", size=1, start=(0, 0), end=(2, 2)),
        Allocation(id="j", size=1, start=(3, 0), end=(4, 1)),
        Allocation(id="k", size=1, start=(0, 3), end=(1, 4)),
    )
    assert get_pressure(pinwheel, work_budget=None) == 3
    assert get_closure_pressure(pinwheel) == 2


def test_closure_pressure_cap_raises() -> None:
    allocations = tuple(
        Allocation(id=i, size=1, start=(i, 8 - i, 0), end=(i + 1, 9 - i, 9))
        for i in range(8)
    )
    with pytest.raises(RuntimeError, match="closure_cap"):
        get_closure_pressure(allocations, closure_cap=4)


def test_exact_pressures_reject_mixed_dimensions() -> None:
    mixed = (
        Allocation(id=1, size=8, start=(0, 0), end=(1, 1)),
        Allocation(id=2, size=8, start=(0, 0, 0), end=(1, 1, 1)),
    )
    with pytest.raises(ValueError, match="dimension"):
        get_pressure(mixed, work_budget=None)
    with pytest.raises(ValueError, match="dimension"):
        get_closure_pressure(mixed)


def test_exact_pressures_match_scalar_equivalent_under_lockstep() -> None:
    scalar = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=5, end=8),
    )
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    assert get_pressure(lockstep, work_budget=None) == get_pressure(scalar)
    assert get_closure_pressure(lockstep) == get_pressure(scalar)


def test_per_allocation_pressures_empty() -> None:
    assert get_per_allocation_pressure(()) == {}
    assert get_per_allocation_closure_pressure(()) == {}
    assert get_per_allocation_placement_pressure(()) == {}


def test_per_allocation_pressure_scalar() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert get_per_allocation_pressure(allocations) == {1: 150, 2: 150, 3: 25}
    assert get_per_allocation_closure_pressure(allocations) == {1: 150, 2: 150, 3: 25}


def test_per_allocation_pressure_two_plus_two() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    expected = {"a": 72, "b": 80, "c": 48, "d": 80}
    assert get_per_allocation_pressure(two_plus_two) == expected
    assert get_per_allocation_closure_pressure(two_plus_two) == expected
    assert max(expected.values()) == get_pressure(two_plus_two)


def test_per_allocation_pressure_scalar_ignores_work_budget() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
    )
    assert get_per_allocation_pressure(allocations, work_budget=1) == {1: 150, 2: 150}


def test_per_allocation_pressure_work_budget_exceeded_raises() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        get_per_allocation_pressure(two_plus_two, work_budget=1)


def test_per_allocation_pressure_unbudgeted_matches_default() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert get_per_allocation_pressure(
        two_plus_two, work_budget=None
    ) == get_per_allocation_pressure(two_plus_two)


def test_per_allocation_closure_below_pinned_without_common_cut() -> None:
    pinwheel = (
        Allocation(id="i", size=1, start=(0, 0), end=(2, 2)),
        Allocation(id="j", size=1, start=(3, 0), end=(4, 1)),
        Allocation(id="k", size=1, start=(0, 3), end=(1, 4)),
    )
    assert get_per_allocation_pressure(pinwheel) == {"i": 3, "j": 3, "k": 3}
    assert get_per_allocation_closure_pressure(pinwheel) == {"i": 2, "j": 2, "k": 2}


def test_per_allocation_pressure_matches_scalar_equivalent_under_lockstep() -> None:
    scalar = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=5, end=8),
    )
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    assert get_per_allocation_pressure(lockstep) == get_per_allocation_pressure(scalar)


def test_per_allocation_closure_pressure_cap_raises() -> None:
    allocations = tuple(
        Allocation(id=i, size=1, start=(i, 8 - i, 0), end=(i + 1, 9 - i, 9))
        for i in range(8)
    )
    with pytest.raises(RuntimeError, match="closure_cap"):
        get_per_allocation_closure_pressure(allocations, closure_cap=4)


def test_per_allocation_pressures_reject_duplicate_ids() -> None:
    duplicated = (
        Allocation(id=1, size=8, start=0, end=2, offset=0),
        Allocation(id=1, size=8, start=1, end=3, offset=8),
    )
    with pytest.raises(ValueError, match="unique"):
        get_per_allocation_pressure(duplicated)
    with pytest.raises(ValueError, match="unique"):
        get_per_allocation_closure_pressure(duplicated)
    with pytest.raises(ValueError, match="unique"):
        get_per_allocation_placement_pressure(duplicated)


def test_per_allocation_placement_pressure_requires_offsets() -> None:
    unplaced = (Allocation(id=1, size=8, start=0, end=2),)
    with pytest.raises(ValueError, match="placed"):
        get_per_allocation_placement_pressure(unplaced)


def test_placement_pressure_empty_is_zero() -> None:
    assert get_placement_pressure(()) == 0


def test_placement_pressure_is_highest_occupied_address() -> None:
    placed = (
        Allocation(id="x", size=5, start=0, end=2, offset=0),
        Allocation(id="y", size=50, start=1, end=3, offset=5),
        Allocation(id="z", size=5, start=2, end=4, offset=0),
    )
    assert get_placement_pressure(placed) == 55


def test_placement_pressure_requires_offsets() -> None:
    unplaced = (Allocation(id=1, size=8, start=0, end=2),)
    with pytest.raises(ValueError, match="placed"):
        get_placement_pressure(unplaced)


def test_per_allocation_placement_pressure_max_equals_peak() -> None:
    placed = (
        Allocation(id="x", size=5, start=0, end=2, offset=0),
        Allocation(id="y", size=50, start=1, end=3, offset=5),
        Allocation(id="z", size=5, start=2, end=4, offset=0),
    )
    peaks = get_per_allocation_placement_pressure(placed)
    assert peaks == {"x": 55, "y": 55, "z": 55}
    assert max(peaks.values()) == 55


def test_per_allocation_placement_pressure_clique_cap_tightens() -> None:
    placed = (
        Allocation(id="a", size=1, start=0, end=2, offset=0),
        Allocation(id="b", size=1, start=1, end=4, offset=10),
        Allocation(id="c", size=10, start=3, end=5, offset=0),
    )
    assert get_per_allocation_placement_pressure(placed) == {"a": 11, "b": 11, "c": 11}
    capped = get_per_allocation_placement_pressure(placed, clique_cap=True)
    assert capped == {"a": 2, "b": 11, "c": 11}


def _brute_antichain(allocations: tuple[Allocation, ...]) -> int:
    best = 0
    for count in range(1, len(allocations) + 1):
        for combo in combinations(allocations, count):
            if all(a.overlaps_temporally(b) for a, b in combinations(combo, 2)):
                best = max(best, sum(a.size for a in combo))
    return best


def _brute_closure(allocations: tuple[Allocation, ...]) -> int:
    best = 0
    for count in range(1, len(allocations) + 1):
        for combo in combinations(allocations, count):
            starts = (a.start for a in combo)
            cut = tuple(max(parts) for parts in zip(*starts, strict=True))
            live = all(
                not all(e <= c for e, c in zip(a.end, cut, strict=True)) for a in combo
            )
            if live:
                best = max(best, sum(a.size for a in combo))
    return best


def _random_instance(rng: Random) -> tuple[Allocation, ...]:
    dim = rng.choice((2, 3))
    allocations = []
    for i in range(rng.randint(1, 9)):
        start = tuple(rng.randint(0, 5) for _ in range(dim))
        delta = [rng.randint(0, 3) for _ in range(dim)]
        if sum(delta) == 0:
            delta[rng.randrange(dim)] = 1
        end = tuple(s + x for s, x in zip(start, delta, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 100), start=start, end=end)
        )
    return tuple(allocations)


def test_antichain_pressure_matches_brute_force() -> None:
    rng = Random(7)
    for _ in range(150):
        allocations = _random_instance(rng)
        assert get_pressure(allocations, work_budget=None) == _brute_antichain(
            allocations
        )


def test_closure_pressure_matches_brute_force_and_bound_order() -> None:
    rng = Random(11)
    for _ in range(150):
        allocations = _random_instance(rng)
        antichain = get_pressure(allocations, work_budget=None)
        closure = get_closure_pressure(allocations)
        assert closure == _brute_closure(allocations)
        assert closure <= antichain
        assert get_pressure(allocations) == antichain


def _brute_pinned_antichain(
    allocations: tuple[Allocation, ...],
) -> dict[int | str, int]:
    peaks = {}
    for pin in allocations:
        others = tuple(a for a in allocations if a.id != pin.id)
        best = pin.size
        for count in range(1, len(others) + 1):
            for combo in combinations(others, count):
                group = (pin, *combo)
                if all(a.overlaps_temporally(b) for a, b in combinations(group, 2)):
                    best = max(best, sum(a.size for a in group))
        peaks[pin.id] = best
    return peaks


def _brute_pinned_closure(
    allocations: tuple[Allocation, ...],
) -> dict[int | str, int]:
    peaks = {}
    for pin in allocations:
        others = tuple(a for a in allocations if a.id != pin.id)
        best = pin.size
        for count in range(1, len(others) + 1):
            for combo in combinations(others, count):
                group = (pin, *combo)
                starts = (a.start for a in group)
                cut = tuple(max(parts) for parts in zip(*starts, strict=True))
                live = all(
                    not all(e <= c for e, c in zip(a.end, cut, strict=True))
                    for a in group
                )
                if live:
                    best = max(best, sum(a.size for a in group))
        peaks[pin.id] = best
    return peaks


def test_per_allocation_pressures_match_brute_force() -> None:
    rng = Random(13)
    for _ in range(60):
        allocations = _random_instance(rng)
        pinned = get_per_allocation_pressure(allocations)
        closure = get_per_allocation_closure_pressure(allocations)
        assert pinned == _brute_pinned_antichain(allocations)
        assert closure == _brute_pinned_closure(allocations)


def test_per_allocation_bound_order_and_peak_identities() -> None:
    rng = Random(17)
    allocator = OmniAllocator()
    for _ in range(40):
        allocations = _random_instance(rng)
        pinned = get_per_allocation_pressure(allocations)
        closure = get_per_allocation_closure_pressure(allocations)
        placed = allocator.allocate(allocations)
        placement = get_per_allocation_placement_pressure(placed)
        capped = get_per_allocation_placement_pressure(placed, clique_cap=True)
        assert max(pinned.values()) == get_pressure(allocations)
        assert max(closure.values()) == get_closure_pressure(allocations)
        assert max(placement.values()) == get_placement_pressure(placed)
        for alloc_id in pinned:
            assert closure[alloc_id] <= pinned[alloc_id]
            assert pinned[alloc_id] <= capped[alloc_id] <= placement[alloc_id]
