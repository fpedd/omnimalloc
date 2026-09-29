#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc import analysis
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.analysis import conflicts, try_linearize
from omnimalloc.common.constants import DEFAULT_WORK_BUDGET
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation


def _overlap_map(allocations: tuple[Allocation, ...]) -> dict[object, set[object]]:
    conflict_map = conflicts(allocations, None)
    return {a.id: set(conflict_map[a.id]) for a in allocations}


def _a(i: object, start: object, end: object) -> Allocation:
    return Allocation(id=i, size=8, start=start, end=end)


TWO_PLUS_TWO = (
    _a("a", (0, 0), (1, 0)),
    _a("b", (1, 0), (2, 0)),
    _a("c", (0, 0), (0, 1)),
    _a("d", (0, 1), (0, 2)),
)
SKEWED = tuple(_a(i, (i, 2 * i), (i + 1, 2 * i + 2)) for i in range(4))
DEGENERATE = tuple(_a(i, (i, i, 0), (i + 1, i + 1, 0)) for i in range(4))


@pytest.mark.parametrize(
    ("allocations", "work_budget"),
    [
        pytest.param(
            tuple(_a(i, (i, i), (i + 1, i + 1)) for i in range(4)),
            DEFAULT_WORK_BUDGET,
            id="ordered_chain",
        ),
        pytest.param(
            (_a(1, (0, 5), (1, 6)), _a(2, (2, 0), (3, 1))),
            DEFAULT_WORK_BUDGET,
            id="concurrent_pair",
        ),
        pytest.param(SKEWED, None, id="unbounded_budget"),
        pytest.param(DEGENERATE, 2 * 4 * 3, id="budget_covering_the_reduction"),
        pytest.param(
            tuple(_a(i, (i,) * 4, (i + 2,) * 4) for i in range(4)),
            DEFAULT_WORK_BUDGET,
            id="all_duplicate_columns",
        ),
        pytest.param(
            (
                _a(1, (0, 0), (2, 1)),
                _a(2, (0, 0), (2, 1)),
                _a(3, (2, 1), (3, 2)),
                _a(4, (2, 1), (3, 2)),
            ),
            DEFAULT_WORK_BUDGET,
            id="duplicate_clock_values",
        ),
    ],
)
def test_linearization_keeps_the_conflicts(
    allocations: tuple[Allocation, ...], work_budget: int | None
) -> None:
    linearized = try_linearize(allocations, work_budget=work_budget)
    assert linearized is not None
    assert all(a.dim == 1 for a in linearized)
    assert _overlap_map(linearized) == _overlap_map(allocations)


@pytest.mark.parametrize(
    "allocations",
    [
        pytest.param(TWO_PLUS_TWO, id="two_plus_two"),
        pytest.param(
            tuple(
                _a(
                    a.id,
                    (a.start[0], a.start[0], 3, a.start[1]),
                    (a.end[0], a.end[0], 3, a.end[1]),
                )
                for a in TWO_PLUS_TWO
            ),
            id="two_plus_two_under_reducible_columns",
        ),
        pytest.param(
            (
                _a(0, (0, 0), (1, 0)),
                _a(1, (0, 0), (0, 1)),
                _a(2, (2, 0), (3, 0)),
                _a(3, (0, 2), (0, 3)),
            ),
            id="non_interval_order",
        ),
    ],
)
def test_a_non_interval_order_does_not_linearize(
    allocations: tuple[Allocation, ...],
) -> None:
    assert try_linearize(allocations) is None
    assert try_linearize(allocations, work_budget=None) is None


@pytest.mark.parametrize(
    ("allocations", "work_budget", "error", "match"),
    [
        (SKEWED, 1, RuntimeError, "exceeds work_budget"),
        (DEGENERATE, 0, RuntimeError, "exceeds work_budget"),
        ((), -1, ValueError, "work_budget must be non-negative"),
        ((_a(1, 0, 4), _a(2, (0, 1), (2, 2))), None, ValueError, "dimension"),
    ],
)
def test_linearize_rejects(
    allocations: tuple[Allocation, ...],
    work_budget: int | None,
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        try_linearize(allocations, work_budget=work_budget)


def test_scalar_input_returned_unchanged() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=2, end=6),
    )
    assert try_linearize(allocations) is allocations


def test_empty_input_returned_unchanged() -> None:
    assert try_linearize(()) == ()


def test_single_varying_column_behaves_like_the_scalar_instance() -> None:
    scalar = tuple(Allocation(id=i, size=8, start=i, end=i + 2) for i in range(4))
    padded = tuple(
        Allocation(id=i, size=8, start=(3, i, 7), end=(3, i + 2, 7)) for i in range(4)
    )
    linearized = try_linearize(padded, work_budget=None)
    assert linearized is not None
    assert all(a.dim == 1 for a in linearized)
    assert _overlap_map(linearized) == _overlap_map(scalar)


def test_partial_column_reduction_matches_the_unpadded_instance() -> None:
    plain = tuple(
        Allocation(id=i, size=8, start=(i, 2 * i), end=(i + 1, 2 * i + 2))
        for i in range(4)
    )
    padded = tuple(
        Allocation(
            id=i, size=8, start=(i, i, 5, 2 * i), end=(i + 1, i + 1, 5, 2 * i + 2)
        )
        for i in range(4)
    )
    linearized = try_linearize(padded, work_budget=None)
    assert linearized is not None
    assert _overlap_map(linearized) == _overlap_map(padded)
    plain_linearized = try_linearize(plain, work_budget=None)
    assert plain_linearized is not None
    assert [(a.start, a.end) for a in linearized] == [
        (a.start, a.end) for a in plain_linearized
    ]


def test_linearize_preserves_metadata() -> None:
    allocations = (
        Allocation(id="x", size=64, start=(0, 0), end=(2, 1), offset=128),
        Allocation(id="y", size=32, start=(2, 1), end=(3, 2)),
    )
    linearized = try_linearize(allocations)
    assert linearized is not None
    assert [(a.id, a.size, a.offset) for a in linearized] == [
        ("x", 64, 128),
        ("y", 32, None),
    ]


def test_linearize_merges_distinct_starts_with_equal_predecessors() -> None:
    allocations = (
        Allocation(id=1, size=8, start=(0, 0), end=(1, 1)),
        Allocation(id=2, size=8, start=(1, 2), end=(2, 3)),
        Allocation(id=3, size=8, start=(2, 1), end=(3, 3)),
    )
    linearized = try_linearize(allocations)
    assert linearized is not None
    assert linearized[1].start == linearized[2].start
    assert _overlap_map(linearized) == _overlap_map(allocations)


def test_linearize_conflict_parity_on_large_lockstep_instance() -> None:
    rng = random.Random(11)
    items = []
    for i in range(2000):
        start = rng.randint(0, 500)
        items.append(
            Allocation(
                id=i,
                size=rng.randint(1, 64),
                start=start,
                end=start + rng.randint(1, 40),
            )
        )
    scalar = tuple(items)
    lockstep = tuple(
        Allocation(id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end))
        for a in scalar
    )
    linearized = try_linearize(lockstep)
    assert linearized is not None
    assert _overlap_map(linearized) == _overlap_map(scalar)


def test_linearize_rejects_random_concurrent_instances() -> None:
    rng = random.Random(4)
    allocations = []
    for i in range(50):
        thread = rng.randint(0, 3)
        local = rng.randint(0, 100)
        start = [0, 0, 0, 0]
        start[thread] = local
        end = list(start)
        end[thread] = local + rng.randint(1, 10)
        allocations.append(Allocation(id=i, size=8, start=tuple(start), end=tuple(end)))
    assert try_linearize(tuple(allocations)) is None


def test_linearize_unlocks_supermalloc() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=50, start=(1, 0), end=(3, 2)),
        Allocation(id=3, size=100, start=(2, 1), end=(4, 3)),
        Allocation(id=4, size=50, start=(4, 3), end=(5, 4)),
    )
    linearized = try_linearize(allocations)
    assert linearized is not None
    placed = SupermallocAllocator().allocate(linearized)
    validate_allocation(Pool(id="p", allocations=placed))


def test_linearize_module_hidden_from_analysis_namespace() -> None:
    assert not hasattr(analysis, "linearize")
