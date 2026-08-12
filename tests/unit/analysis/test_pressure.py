#
# SPDX-License-Identifier: Apache-2.0
#

from itertools import combinations
from random import Random

import pytest
from omnimalloc import allocate
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.analysis import (
    antichain_pressure,
    antichain_pressure_per_allocation,
    closure_pressure,
    closure_pressure_per_allocation,
    placement_pressure,
    placement_pressure_per_allocation,
)
from omnimalloc.primitives import Allocation


def test_pressure_empty_is_zero() -> None:
    assert antichain_pressure(()) == 0


def test_pressure_scalar_overlap() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert antichain_pressure(allocations) == 150


def test_pressure_scalar_disjoint() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=2),
        Allocation(id=2, size=50, start=2, end=4),
    )
    assert antichain_pressure(allocations) == 100


def test_pressure_linearizable_vector_is_exact() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=50, start=(1, 0), end=(3, 2)),
        Allocation(id=3, size=25, start=(3, 2), end=(4, 3)),
    )
    assert antichain_pressure(allocations) == 150


def test_pressure_non_linearizable_is_exact() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert antichain_pressure(two_plus_two) == 16 + 64


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
    assert antichain_pressure(lockstep) == antichain_pressure(scalar)


def test_pressure_scalar_ignores_work_budget() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
    )
    assert antichain_pressure(allocations, work_budget=1) == 150


def test_pressure_work_budget_exceeded_raises() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        antichain_pressure(two_plus_two, work_budget=1)


def test_pressure_negative_work_budget_rejected() -> None:
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        antichain_pressure((), work_budget=-1)


def test_closure_pressure_negative_cap_rejected() -> None:
    with pytest.raises(ValueError, match="closure_cap must be non-negative"):
        closure_pressure((), closure_cap=-1)


def test_closure_pressure_none_cap_enumerates_unbounded() -> None:
    allocations = tuple(
        Allocation(id=i, size=1, start=(i, 8 - i, 0), end=(i + 1, 9 - i, 9))
        for i in range(8)
    )
    assert closure_pressure(allocations, closure_cap=None) == closure_pressure(
        allocations
    )


def test_pressure_total_size_overflow_raises() -> None:
    allocations = tuple(Allocation(id=i, size=2**62, start=0, end=1) for i in range(4))
    with pytest.raises(ValueError, match="int64"):
        antichain_pressure(allocations)


def test_closure_pressure_total_size_overflow_raises() -> None:
    allocations = tuple(Allocation(id=i, size=2**62, start=0, end=1) for i in range(4))
    with pytest.raises(ValueError, match="int64"):
        closure_pressure(allocations)
    with pytest.raises(ValueError, match="int64"):
        closure_pressure_per_allocation(allocations)


def test_pressure_unbudgeted_empty_is_zero() -> None:
    assert antichain_pressure((), work_budget=None) == 0


def test_closure_pressure_empty_is_zero() -> None:
    assert closure_pressure(()) == 0


def test_exact_pressures_match_scalar_sweep() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert antichain_pressure(allocations, work_budget=None) == 150
    assert closure_pressure(allocations) == 150


def test_pressure_unbudgeted_two_plus_two_exact() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert antichain_pressure(two_plus_two, work_budget=None) == 16 + 64


def test_closure_pressure_below_antichain_without_common_cut() -> None:
    pinwheel = (
        Allocation(id="i", size=1, start=(0, 0), end=(2, 2)),
        Allocation(id="j", size=1, start=(3, 0), end=(4, 1)),
        Allocation(id="k", size=1, start=(0, 3), end=(1, 4)),
    )
    assert antichain_pressure(pinwheel, work_budget=None) == 3
    assert closure_pressure(pinwheel) == 2


def test_closure_pressure_cap_raises() -> None:
    allocations = tuple(
        Allocation(id=i, size=1, start=(i, 8 - i, 0), end=(i + 1, 9 - i, 9))
        for i in range(8)
    )
    with pytest.raises(RuntimeError, match="closure_cap"):
        closure_pressure(allocations, closure_cap=4)


def test_exact_pressures_reject_mixed_dimensions() -> None:
    mixed = (
        Allocation(id=1, size=8, start=(0, 0), end=(1, 1)),
        Allocation(id=2, size=8, start=(0, 0, 0), end=(1, 1, 1)),
    )
    with pytest.raises(ValueError, match="dimension"):
        antichain_pressure(mixed, work_budget=None)
    with pytest.raises(ValueError, match="dimension"):
        closure_pressure(mixed)


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
    assert antichain_pressure(lockstep, work_budget=None) == antichain_pressure(scalar)
    assert closure_pressure(lockstep) == antichain_pressure(scalar)


def test_per_allocation_pressures_empty() -> None:
    assert antichain_pressure_per_allocation(()) == {}
    assert closure_pressure_per_allocation(()) == {}
    assert placement_pressure_per_allocation(()) == {}


def test_per_allocation_pressure_scalar() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert antichain_pressure_per_allocation(allocations) == {1: 150, 2: 150, 3: 25}
    assert closure_pressure_per_allocation(allocations) == {1: 150, 2: 150, 3: 25}


def test_per_allocation_pressure_two_plus_two() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    expected = {"a": 72, "b": 80, "c": 48, "d": 80}
    assert antichain_pressure_per_allocation(two_plus_two) == expected
    assert closure_pressure_per_allocation(two_plus_two) == expected
    assert max(expected.values()) == antichain_pressure(two_plus_two)


def test_per_allocation_pressure_scalar_ignores_work_budget() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
    )
    assert antichain_pressure_per_allocation(allocations, work_budget=1) == {
        1: 150,
        2: 150,
    }


def test_per_allocation_pressure_work_budget_exceeded_raises() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        antichain_pressure_per_allocation(two_plus_two, work_budget=1)


def test_per_allocation_pressure_unbudgeted_matches_default() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert antichain_pressure_per_allocation(
        two_plus_two, work_budget=None
    ) == antichain_pressure_per_allocation(two_plus_two)


def test_per_allocation_closure_below_pinned_without_common_cut() -> None:
    pinwheel = (
        Allocation(id="i", size=1, start=(0, 0), end=(2, 2)),
        Allocation(id="j", size=1, start=(3, 0), end=(4, 1)),
        Allocation(id="k", size=1, start=(0, 3), end=(1, 4)),
    )
    assert antichain_pressure_per_allocation(pinwheel) == {"i": 3, "j": 3, "k": 3}
    assert closure_pressure_per_allocation(pinwheel) == {"i": 2, "j": 2, "k": 2}


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
    assert antichain_pressure_per_allocation(
        lockstep
    ) == antichain_pressure_per_allocation(scalar)


def test_per_allocation_closure_pressure_cap_raises() -> None:
    allocations = tuple(
        Allocation(id=i, size=1, start=(i, 8 - i, 0), end=(i + 1, 9 - i, 9))
        for i in range(8)
    )
    with pytest.raises(RuntimeError, match="closure_cap"):
        closure_pressure_per_allocation(allocations, closure_cap=4)


def test_per_allocation_pressures_reject_duplicate_ids() -> None:
    duplicated = (
        Allocation(id=1, size=8, start=0, end=2, offset=0),
        Allocation(id=1, size=8, start=1, end=3, offset=8),
    )
    with pytest.raises(ValueError, match="unique"):
        antichain_pressure_per_allocation(duplicated)
    with pytest.raises(ValueError, match="unique"):
        closure_pressure_per_allocation(duplicated)
    with pytest.raises(ValueError, match="unique"):
        placement_pressure_per_allocation(duplicated)


def test_per_allocation_placement_pressure_requires_offsets() -> None:
    unplaced = (Allocation(id=1, size=8, start=0, end=2),)
    with pytest.raises(ValueError, match="placed"):
        placement_pressure_per_allocation(unplaced)


def test_placement_pressure_empty_is_zero() -> None:
    assert placement_pressure(()) == 0


def test_placement_pressure_is_highest_occupied_address() -> None:
    placed = (
        Allocation(id="x", size=5, start=0, end=2, offset=0),
        Allocation(id="y", size=50, start=1, end=3, offset=5),
        Allocation(id="z", size=5, start=2, end=4, offset=0),
    )
    assert placement_pressure(placed) == 55


def test_placement_pressure_requires_offsets() -> None:
    unplaced = (Allocation(id=1, size=8, start=0, end=2),)
    with pytest.raises(ValueError, match="placed"):
        placement_pressure(unplaced)


def test_placement_pressure_rejects_mixed_dimensions() -> None:
    mixed = (
        Allocation(id=1, size=8, start=(0, 0), end=(1, 1), offset=0),
        Allocation(id=2, size=8, start=(0, 0, 0), end=(1, 1, 1), offset=8),
    )
    with pytest.raises(ValueError, match="dimension"):
        placement_pressure(mixed)


def test_per_allocation_placement_pressure_max_equals_peak() -> None:
    placed = (
        Allocation(id="x", size=5, start=0, end=2, offset=0),
        Allocation(id="y", size=50, start=1, end=3, offset=5),
        Allocation(id="z", size=5, start=2, end=4, offset=0),
    )
    peaks = placement_pressure_per_allocation(placed)
    assert peaks == {"x": 55, "y": 55, "z": 55}
    assert max(peaks.values()) == 55


def test_per_allocation_placement_pressure_budget_raises() -> None:
    placed = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0), offset=96),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0), offset=96),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1), offset=0),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2), offset=32),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        placement_pressure_per_allocation(placed, work_budget=1)


def test_per_allocation_placement_pressure_unbounded_budget_computes() -> None:
    placed = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0), offset=96),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0), offset=96),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1), offset=0),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2), offset=32),
    )
    expected = {"a": 104, "b": 112, "c": 112, "d": 112}
    assert placement_pressure_per_allocation(placed, work_budget=None) == expected
    assert placement_pressure_per_allocation(placed) == expected


def _brute_antichain(allocations: tuple[Allocation, ...]) -> int:
    best = 0
    for count in range(1, len(allocations) + 1):
        for combo in combinations(allocations, count):
            if all(a.conflicts_with(b) for a, b in combinations(combo, 2)):
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
        assert antichain_pressure(allocations, work_budget=None) == _brute_antichain(
            allocations
        )


def test_closure_pressure_matches_brute_force_and_bound_order() -> None:
    rng = Random(11)
    for _ in range(150):
        allocations = _random_instance(rng)
        antichain = antichain_pressure(allocations, work_budget=None)
        closure = closure_pressure(allocations)
        assert closure == _brute_closure(allocations)
        assert closure <= antichain
        assert antichain_pressure(allocations) == antichain


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
                if all(a.conflicts_with(b) for a, b in combinations(group, 2)):
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
        pinned = antichain_pressure_per_allocation(allocations)
        closure = closure_pressure_per_allocation(allocations)
        assert pinned == _brute_pinned_antichain(allocations)
        assert closure == _brute_pinned_closure(allocations)


def _brute_placement(allocations: tuple[Allocation, ...]) -> dict[int | str, int]:
    peaks = {}
    for pin in allocations:
        top = pin.offset + pin.size
        for other in allocations:
            if other is not pin and pin.conflicts_with(other):
                top = max(top, other.offset + other.size)
        peaks[pin.id] = top
    return peaks


def test_per_allocation_placement_pressure_matches_brute_force() -> None:
    rng = Random(19)
    allocator = OmniAllocator()
    for _ in range(60):
        allocations = _random_instance(rng)
        placed = allocator.allocate(allocations)
        assert placement_pressure_per_allocation(placed) == _brute_placement(placed)
        scrambled = tuple(a.with_offset(rng.randint(0, 300)) for a in allocations)
        assert placement_pressure_per_allocation(
            scrambled, work_budget=None
        ) == _brute_placement(scrambled)


def _assert_matches_brute(allocations: tuple[Allocation, ...]) -> None:
    assert placement_pressure_per_allocation(allocations) == _brute_placement(
        allocations
    )


def test_per_allocation_placement_pressure_on_a_shared_offset() -> None:
    _assert_matches_brute(
        tuple(Allocation(id=i, size=8, start=i, end=i + 3, offset=0) for i in range(40))
    )


def test_per_allocation_placement_pressure_on_one_shared_instant() -> None:
    _assert_matches_brute(
        tuple(Allocation(id=i, size=8, start=0, end=1, offset=8 * i) for i in range(40))
    )


def test_per_allocation_placement_pressure_on_a_nested_staircase() -> None:
    _assert_matches_brute(
        tuple(
            Allocation(id=i, size=4, start=i, end=80 - i, offset=4 * i)
            for i in range(40)
        )
    )


def test_per_allocation_placement_pressure_on_a_reversed_staircase() -> None:
    _assert_matches_brute(
        tuple(
            Allocation(id=i, size=4, start=i, end=80 - i, offset=4 * (40 - i))
            for i in range(40)
        )
    )


def test_per_allocation_placement_pressure_on_disjoint_unit_lifetimes() -> None:
    _assert_matches_brute(
        tuple(Allocation(id=i, size=8, start=i, end=i + 1, offset=0) for i in range(40))
    )


def test_per_allocation_placement_pressure_under_one_tall_spanner() -> None:
    _assert_matches_brute(
        (
            Allocation(id="span", size=1, start=0, end=100, offset=0),
            *(
                Allocation(id=i, size=100, start=i, end=i + 1, offset=1)
                for i in range(1, 40)
            ),
        )
    )


def test_per_allocation_placement_pressure_matches_brute_force_on_scalar_time() -> None:
    rng = Random(23)
    for _ in range(200):
        horizon = rng.choice((1, 2, 5, 40))
        drawn = []
        for i in range(rng.randint(1, 40)):
            start = rng.randint(0, horizon)
            drawn.append(
                Allocation(
                    id=i,
                    size=rng.randint(1, 16),
                    start=start,
                    end=start + rng.randint(1, 4),
                    offset=rng.choice((0, 0, rng.randint(0, 64))),
                )
            )
        allocations = tuple(drawn)
        assert placement_pressure_per_allocation(
            allocations, work_budget=None
        ) == _brute_placement(allocations)


def test_antichain_pressure_column_collapse_admits_wide_lockstep_clocks() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(i,) * 64, end=(i + 2,) * 64)
        for i in range(3000)
    )
    assert antichain_pressure(allocations) == 16
    assert antichain_pressure(allocations, work_budget=2 * 3000 * 64) == 16
    with pytest.raises(RuntimeError, match="work_budget"):
        antichain_pressure(allocations, work_budget=0)


def test_per_allocation_placement_pressure_default_budget_admits_wide_sweep() -> None:
    zero = (0,) * 64
    ahead = (1, *(0,) * 63)
    aside = (0, 1, *(0,) * 62)
    clique = [
        Allocation(id=i, size=8, start=zero, end=(1,) * 64, offset=8 * i)
        for i in range(3000)
    ]
    sweep_forcing_two_plus_two = [
        Allocation(id="a", size=8, start=zero, end=ahead, offset=24_000),
        Allocation(id="b", size=8, start=ahead, end=(2, *(0,) * 63), offset=24_008),
        Allocation(id="c", size=8, start=zero, end=aside, offset=24_016),
        Allocation(id="d", size=8, start=aside, end=(0, 2, *(0,) * 62), offset=24_024),
    ]
    allocations = tuple(clique + sweep_forcing_two_plus_two)
    peaks = placement_pressure_per_allocation(allocations)
    assert peaks["c"] == 24_024
    assert set(peaks.values()) == {24_024, 24_032}


def test_per_allocation_bound_order_and_peak_identities() -> None:
    rng = Random(17)
    allocator = OmniAllocator()
    for _ in range(40):
        allocations = _random_instance(rng)
        pinned = antichain_pressure_per_allocation(allocations)
        closure = closure_pressure_per_allocation(allocations)
        placed = allocator.allocate(allocations)
        placement = placement_pressure_per_allocation(placed)
        assert max(pinned.values()) == antichain_pressure(allocations)
        assert max(closure.values()) == closure_pressure(allocations)
        assert max(placement.values()) == placement_pressure(placed)
        for alloc_id in pinned:
            assert closure[alloc_id] <= pinned[alloc_id]
            assert pinned[alloc_id] <= placement[alloc_id]


def test_placement_pressure_per_allocation_paints_nested_lifetimes() -> None:
    allocations = (
        Allocation(id="long", size=8, start=0, end=10, offset=0),
        Allocation(id="tall", size=100, start=2, end=4, offset=8),
        Allocation(id="short", size=10, start=6, end=8, offset=8),
    )
    peaks = placement_pressure_per_allocation(allocations)
    assert peaks == {"long": 108, "tall": 108, "short": 18}


def test_placement_pressure_per_allocation_ignores_uncovered_slots() -> None:
    allocations = (
        Allocation(id="a", size=8, start=0, end=2, offset=0),
        Allocation(id="b", size=64, start=8, end=10, offset=0),
    )
    peaks = placement_pressure_per_allocation(allocations)
    assert peaks == {"a": 8, "b": 64}


def test_placement_pressure_per_allocation_survives_degenerate_columns() -> None:
    rng = Random(23)
    for _ in range(40):
        base = [
            (i, rng.randint(1, 40), rng.randint(0, 20), rng.randint(1, 6))
            for i in range(rng.randint(1, 25))
        ]
        scalar = tuple(
            Allocation(id=i, size=z, start=s, end=s + d, offset=8 * i)
            for i, z, s, d in base
        )
        padded = tuple(
            Allocation(id=i, size=z, start=(s, 0), end=(s + d, 0), offset=8 * i)
            for i, z, s, d in base
        )
        lockstep = tuple(
            Allocation(id=i, size=z, start=(s,) * 3, end=(s + d,) * 3, offset=8 * i)
            for i, z, s, d in base
        )
        expected = placement_pressure_per_allocation(scalar)
        assert placement_pressure_per_allocation(padded) == expected
        assert placement_pressure_per_allocation(lockstep) == expected


def test_closure_bound_is_strictly_looser_than_the_antichain_bound() -> None:
    allocations = (
        Allocation(id=0, size=20, start=(1, 0, 0), end=(1, 2, 1)),
        Allocation(id=1, size=20, start=(0, 2, 0), end=(1, 2, 1)),
        Allocation(id=2, size=30, start=(1, 0, 0), end=(1, 2, 0)),
    )
    assert all(
        a.conflicts_with(b)
        for a, b in ((allocations[0], allocations[1]), (allocations[0], allocations[2]))
    )
    assert closure_pressure(allocations) == 50
    assert antichain_pressure(allocations, work_budget=None) == 70


def test_the_bound_chain_holds_on_the_no_common_cut_clique() -> None:
    allocations = (
        Allocation(id=0, size=20, start=(1, 0, 0), end=(1, 2, 1)),
        Allocation(id=1, size=20, start=(0, 2, 0), end=(1, 2, 1)),
        Allocation(id=2, size=30, start=(1, 0, 0), end=(1, 2, 0)),
    )
    placed = allocate(allocations, "omni")
    assert (
        closure_pressure(allocations)
        <= antichain_pressure(allocations, work_budget=None)
        <= placement_pressure(placed)
    )
