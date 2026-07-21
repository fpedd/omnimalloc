#
# SPDX-License-Identifier: Apache-2.0
#

from itertools import combinations
from random import Random

import pytest
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.analysis import (
    antichain_pressure,
    antichain_pressure_per_allocation,
    closure_pressure,
    closure_pressure_per_allocation,
    placement_pressure,
    placement_pressure_per_allocation,
    pressure,
    pressure_per_allocation,
)
from omnimalloc.primitives import Allocation


def test_default_names_alias_antichain() -> None:
    assert pressure is antichain_pressure
    assert pressure_per_allocation is antichain_pressure_per_allocation


def test_pressure_empty_is_zero() -> None:
    assert pressure(()) == 0


def test_pressure_scalar_overlap() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert pressure(allocations) == 150


def test_pressure_scalar_disjoint() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=2),
        Allocation(id=2, size=50, start=2, end=4),
    )
    assert pressure(allocations) == 100


def test_pressure_linearizable_vector_is_exact() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=50, start=(1, 0), end=(3, 2)),
        Allocation(id=3, size=25, start=(3, 2), end=(4, 3)),
    )
    assert pressure(allocations) == 150


def test_pressure_non_linearizable_is_exact() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert pressure(two_plus_two) == 16 + 64


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
    assert pressure(lockstep) == pressure(scalar)


def test_pressure_scalar_ignores_work_budget() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
    )
    assert pressure(allocations, work_budget=1) == 150


def test_pressure_work_budget_exceeded_raises() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        pressure(two_plus_two, work_budget=1)


def test_pressure_negative_work_budget_rejected() -> None:
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        pressure((), work_budget=-1)


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
        pressure(allocations)


def test_pressure_unbudgeted_empty_is_zero() -> None:
    assert pressure((), work_budget=None) == 0


def test_closure_pressure_empty_is_zero() -> None:
    assert closure_pressure(()) == 0


def test_exact_pressures_match_scalar_sweep() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert pressure(allocations, work_budget=None) == 150
    assert closure_pressure(allocations) == 150


def test_pressure_unbudgeted_two_plus_two_exact() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert pressure(two_plus_two, work_budget=None) == 16 + 64


def test_closure_pressure_below_antichain_without_common_cut() -> None:
    pinwheel = (
        Allocation(id="i", size=1, start=(0, 0), end=(2, 2)),
        Allocation(id="j", size=1, start=(3, 0), end=(4, 1)),
        Allocation(id="k", size=1, start=(0, 3), end=(1, 4)),
    )
    assert pressure(pinwheel, work_budget=None) == 3
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
        pressure(mixed, work_budget=None)
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
    assert pressure(lockstep, work_budget=None) == pressure(scalar)
    assert closure_pressure(lockstep) == pressure(scalar)


def test_per_allocation_pressures_empty() -> None:
    assert pressure_per_allocation(()) == {}
    assert closure_pressure_per_allocation(()) == {}
    assert placement_pressure_per_allocation(()) == {}


def test_per_allocation_pressure_scalar() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
        Allocation(id=3, size=25, start=6, end=8),
    )
    assert pressure_per_allocation(allocations) == {1: 150, 2: 150, 3: 25}
    assert closure_pressure_per_allocation(allocations) == {1: 150, 2: 150, 3: 25}


def test_per_allocation_pressure_two_plus_two() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    expected = {"a": 72, "b": 80, "c": 48, "d": 80}
    assert pressure_per_allocation(two_plus_two) == expected
    assert closure_pressure_per_allocation(two_plus_two) == expected
    assert max(expected.values()) == pressure(two_plus_two)


def test_per_allocation_pressure_scalar_ignores_work_budget() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=4),
        Allocation(id=2, size=50, start=2, end=6),
    )
    assert pressure_per_allocation(allocations, work_budget=1) == {1: 150, 2: 150}


def test_per_allocation_pressure_work_budget_exceeded_raises() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        pressure_per_allocation(two_plus_two, work_budget=1)


def test_per_allocation_pressure_unbudgeted_matches_default() -> None:
    two_plus_two = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=16, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=32, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=64, start=(0, 1), end=(0, 2)),
    )
    assert pressure_per_allocation(
        two_plus_two, work_budget=None
    ) == pressure_per_allocation(two_plus_two)


def test_per_allocation_closure_below_pinned_without_common_cut() -> None:
    pinwheel = (
        Allocation(id="i", size=1, start=(0, 0), end=(2, 2)),
        Allocation(id="j", size=1, start=(3, 0), end=(4, 1)),
        Allocation(id="k", size=1, start=(0, 3), end=(1, 4)),
    )
    assert pressure_per_allocation(pinwheel) == {"i": 3, "j": 3, "k": 3}
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
    assert pressure_per_allocation(lockstep) == pressure_per_allocation(scalar)


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
        pressure_per_allocation(duplicated)
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
        assert pressure(allocations, work_budget=None) == _brute_antichain(allocations)


def test_closure_pressure_matches_brute_force_and_bound_order() -> None:
    rng = Random(11)
    for _ in range(150):
        allocations = _random_instance(rng)
        antichain = pressure(allocations, work_budget=None)
        closure = closure_pressure(allocations)
        assert closure == _brute_closure(allocations)
        assert closure <= antichain
        assert pressure(allocations) == antichain


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
        pinned = pressure_per_allocation(allocations)
        closure = closure_pressure_per_allocation(allocations)
        assert pinned == _brute_pinned_antichain(allocations)
        assert closure == _brute_pinned_closure(allocations)


def test_per_allocation_bound_order_and_peak_identities() -> None:
    rng = Random(17)
    allocator = OmniAllocator()
    for _ in range(40):
        allocations = _random_instance(rng)
        pinned = pressure_per_allocation(allocations)
        closure = closure_pressure_per_allocation(allocations)
        placed = allocator.allocate(allocations)
        placement = placement_pressure_per_allocation(placed)
        assert max(pinned.values()) == pressure(allocations)
        assert max(closure.values()) == closure_pressure(allocations)
        assert max(placement.values()) == placement_pressure(placed)
        for alloc_id in pinned:
            assert closure[alloc_id] <= pinned[alloc_id]
            assert pinned[alloc_id] <= placement[alloc_id]
