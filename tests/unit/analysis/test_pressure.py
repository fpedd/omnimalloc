#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Callable
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
)
from omnimalloc.primitives import Allocation


def _a(
    i: object, size: int, start: object, end: object, offset: int | None = None
) -> Allocation:
    return Allocation(id=i, size=size, start=start, end=end, offset=offset)


OVERLAP = (_a(1, 100, 0, 4), _a(2, 50, 2, 6), _a(3, 25, 6, 8))
DISJOINT = (_a(1, 100, 0, 2), _a(2, 50, 2, 4))
LINEARIZABLE = (
    _a(1, 100, (0, 0), (2, 1)),
    _a(2, 50, (1, 0), (3, 2)),
    _a(3, 25, (3, 2), (4, 3)),
)
TWO_PLUS_TWO = (
    _a("a", 8, (0, 0), (1, 0)),
    _a("b", 16, (1, 0), (2, 0)),
    _a("c", 32, (0, 0), (0, 1)),
    _a("d", 64, (0, 1), (0, 2)),
)
# Pairwise concurrent, but no cut holds all three: the closure bound is lower
PINWHEEL = (
    _a("i", 1, (0, 0), (2, 2)),
    _a("j", 1, (3, 0), (4, 1)),
    _a("k", 1, (0, 3), (1, 4)),
)
NO_COMMON_CUT = (
    _a(0, 20, (1, 0, 0), (1, 2, 1)),
    _a(1, 20, (0, 2, 0), (1, 2, 1)),
    _a(2, 30, (1, 0, 0), (1, 2, 0)),
)
WIDE_CLOSURE = tuple(_a(i, 1, (i, 8 - i, 0), (i + 1, 9 - i, 9)) for i in range(8))
OVERFLOW = tuple(_a(i, 2**62, 0, 1) for i in range(4))
MIXED = (_a(1, 8, (0, 0), (1, 1), 0), _a(2, 8, (0, 0, 0), (1, 1, 1), 8))
DUPLICATED = (_a(1, 8, 0, 2, 0), _a(1, 8, 1, 3, 8))
UNPLACED = (_a(1, 8, 0, 2),)
PLACED_TWO_PLUS_TWO = tuple(
    a.with_offset(offset)
    for a, offset in zip(TWO_PLUS_TWO, (96, 96, 0, 32), strict=True)
)


@pytest.mark.parametrize(
    ("allocations", "antichain", "closure"),
    [
        pytest.param((), 0, 0, id="empty"),
        pytest.param(OVERLAP, 150, 150, id="overlap"),
        pytest.param(DISJOINT, 100, 100, id="disjoint"),
        pytest.param(LINEARIZABLE, 150, 150, id="linearizable"),
        pytest.param(TWO_PLUS_TWO, 80, 80, id="two_plus_two"),
        pytest.param(PINWHEEL, 3, 2, id="pinwheel"),
        pytest.param(NO_COMMON_CUT, 70, 50, id="no_common_cut"),
        pytest.param(WIDE_CLOSURE, 8, 8, id="wide_closure"),
    ],
)
def test_pressures(
    allocations: tuple[Allocation, ...], antichain: int, closure: int
) -> None:
    assert antichain_pressure(allocations) == antichain
    assert antichain_pressure(allocations, work_budget=None) == antichain
    assert closure_pressure(allocations) == closure
    assert closure_pressure(allocations, closure_cap=None) == closure


@pytest.mark.parametrize(
    ("allocations", "antichain", "closure"),
    [
        pytest.param((), {}, {}, id="empty"),
        pytest.param(
            OVERLAP, {1: 150, 2: 150, 3: 25}, {1: 150, 2: 150, 3: 25}, id="overlap"
        ),
        pytest.param(
            TWO_PLUS_TWO,
            {"a": 72, "b": 80, "c": 48, "d": 80},
            {"a": 72, "b": 80, "c": 48, "d": 80},
            id="two_plus_two",
        ),
        pytest.param(
            PINWHEEL, dict.fromkeys("ijk", 3), dict.fromkeys("ijk", 2), id="pinwheel"
        ),
    ],
)
def test_per_allocation_pressures(
    allocations: tuple[Allocation, ...],
    antichain: dict[object, int],
    closure: dict[object, int],
) -> None:
    assert antichain_pressure_per_allocation(allocations) == antichain
    assert antichain_pressure_per_allocation(allocations, work_budget=None) == antichain
    assert closure_pressure_per_allocation(allocations) == closure


@pytest.mark.parametrize(
    ("allocations", "peak", "per_allocation"),
    [
        pytest.param((), 0, {}, id="empty"),
        pytest.param(
            (_a("x", 5, 0, 2, 0), _a("y", 50, 1, 3, 5), _a("z", 5, 2, 4, 0)),
            55,
            {"x": 55, "y": 55, "z": 55},
            id="highest_occupied_address",
        ),
        pytest.param(
            (
                _a("long", 8, 0, 10, 0),
                _a("tall", 100, 2, 4, 8),
                _a("short", 10, 6, 8, 8),
            ),
            108,
            {"long": 108, "tall": 108, "short": 18},
            id="nested_lifetimes",
        ),
        pytest.param(
            (_a("a", 8, 0, 2, 0), _a("b", 64, 8, 10, 0)),
            64,
            {"a": 8, "b": 64},
            id="uncovered_slots",
        ),
        pytest.param(
            PLACED_TWO_PLUS_TWO,
            112,
            {"a": 104, "b": 112, "c": 112, "d": 112},
            id="two_plus_two",
        ),
    ],
)
def test_placement_pressures(
    allocations: tuple[Allocation, ...], peak: int, per_allocation: dict[object, int]
) -> None:
    assert placement_pressure(allocations) == peak
    assert placement_pressure_per_allocation(allocations) == per_allocation
    assert (
        placement_pressure_per_allocation(allocations, work_budget=None)
        == per_allocation
    )


def test_scalar_pressures_ignore_the_work_budget() -> None:
    assert antichain_pressure(OVERLAP[:2], work_budget=1) == 150
    assert antichain_pressure_per_allocation(OVERLAP[:2], work_budget=1) == {
        1: 150,
        2: 150,
    }


def test_pressures_match_the_scalar_instance_under_lockstep() -> None:
    scalar = (_a(1, 100, 0, 4), _a(2, 50, 2, 6), _a(3, 25, 5, 8))
    lockstep = tuple(_a(a.id, a.size, (a.start,) * 2, (a.end,) * 2) for a in scalar)
    assert antichain_pressure(lockstep) == antichain_pressure(scalar)
    assert antichain_pressure(lockstep, work_budget=None) == antichain_pressure(scalar)
    assert closure_pressure(lockstep) == closure_pressure(scalar)
    assert antichain_pressure_per_allocation(
        lockstep
    ) == antichain_pressure_per_allocation(scalar)


BUDGET = {"work_budget": 1}
CAP = {"closure_cap": 4}


@pytest.mark.parametrize(
    ("query", "allocations", "kwargs", "error", "match"),
    [
        (antichain_pressure, TWO_PLUS_TWO, BUDGET, RuntimeError, "work_budget"),
        (antichain_pressure_per_allocation, TWO_PLUS_TWO, BUDGET, RuntimeError, "work"),
        (
            placement_pressure_per_allocation,
            PLACED_TWO_PLUS_TWO,
            BUDGET,
            RuntimeError,
            "work",
        ),
        (closure_pressure, WIDE_CLOSURE, CAP, RuntimeError, "closure_cap"),
        (
            closure_pressure_per_allocation,
            WIDE_CLOSURE,
            CAP,
            RuntimeError,
            "closure_cap",
        ),
        (antichain_pressure, (), {"work_budget": -1}, ValueError, "non-negative"),
        (closure_pressure, (), {"closure_cap": -1}, ValueError, "non-negative"),
        (antichain_pressure, OVERFLOW, {}, ValueError, "int64"),
        (closure_pressure, OVERFLOW, {}, ValueError, "int64"),
        (closure_pressure_per_allocation, OVERFLOW, {}, ValueError, "int64"),
        (antichain_pressure, MIXED, {"work_budget": None}, ValueError, "dimension"),
        (closure_pressure, MIXED, {}, ValueError, "dimension"),
        (placement_pressure, MIXED, {}, ValueError, "dimension"),
        (antichain_pressure_per_allocation, DUPLICATED, {}, ValueError, "unique"),
        (closure_pressure_per_allocation, DUPLICATED, {}, ValueError, "unique"),
        (placement_pressure_per_allocation, DUPLICATED, {}, ValueError, "unique"),
        (placement_pressure, UNPLACED, {}, ValueError, "placed"),
        (placement_pressure_per_allocation, UNPLACED, {}, ValueError, "placed"),
    ],
)
def test_pressure_queries_reject(
    query: Callable[..., object],
    allocations: tuple[Allocation, ...],
    kwargs: dict[str, int | None],
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        query(allocations, **kwargs)


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


@pytest.mark.parametrize(
    "allocations",
    [
        pytest.param(tuple(_a(i, 8, i, i + 3, 0) for i in range(40)), id="shared"),
        pytest.param(tuple(_a(i, 8, 0, 1, 8 * i) for i in range(40)), id="instant"),
        pytest.param(
            tuple(_a(i, 4, i, 80 - i, 4 * i) for i in range(40)), id="staircase"
        ),
        pytest.param(
            tuple(_a(i, 4, i, 80 - i, 4 * (40 - i)) for i in range(40)),
            id="reversed_staircase",
        ),
        pytest.param(tuple(_a(i, 8, i, i + 1, 0) for i in range(40)), id="disjoint"),
        pytest.param(
            (
                _a("span", 1, 0, 100, 0),
                *(_a(i, 100, i, i + 1, 1) for i in range(1, 40)),
            ),
            id="tall_spanner",
        ),
    ],
)
def test_per_allocation_placement_pressure_matches_brute_force_on(
    allocations: tuple[Allocation, ...],
) -> None:
    assert placement_pressure_per_allocation(allocations) == _brute_placement(
        allocations
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
