#
# SPDX-License-Identifier: Apache-2.0
#

from random import Random

import pytest
from omnimalloc.analysis import conflict_degrees, conflict_graph, conflicts
from omnimalloc.primitives import Allocation


def _a(i: object, start: object, end: object) -> Allocation:
    return Allocation(id=i, size=8, start=start, end=end)


OVERLAP = (_a(1, 0, 4), _a(2, 2, 6), _a(3, 6, 8))
CLIQUE = tuple(_a(i, 0, 10) for i in range(4))
DUPLICATED = (_a(1, 0, 2), _a(1, 1, 3))
MIXED = (_a(1, 0, 1), _a(2, (0, 0), (1, 1)))
SKEWED = tuple(_a(i, (i, 2 * i), (i + 10, 2 * i + 10)) for i in range(4))
FIFTY = tuple(_a(i, 0, 1) for i in range(50))


@pytest.mark.parametrize(
    ("allocations", "expected"),
    [
        pytest.param((), {}, id="empty"),
        pytest.param(OVERLAP, {1: {2}, 2: {1}, 3: set()}, id="scalar_overlap"),
        pytest.param((_a(1, 0, 4), _a(2, 4, 6)), {1: set(), 2: set()}, id="touching"),
        pytest.param(
            (_a("a", (0, 0), (1, 0)), _a("b", (0, 0), (0, 1))),
            {"a": {"b"}, "b": {"a"}},
            id="vector_concurrent",
        ),
        pytest.param(
            (_a("a", (0, 0), (1, 0)), _a("b", (1, 0), (2, 0))),
            {"a": set(), "b": set()},
            id="vector_ordered",
        ),
        pytest.param(
            (_a(1, (0, 5), (4, 5)), _a(2, (5, 0), (9, 0))),
            {1: {2}, 2: {1}},
            id="column_pinned_per_row_but_varying_across_rows",
        ),
    ],
)
def test_conflicts(
    allocations: tuple[Allocation, ...], expected: dict[object, set[object]]
) -> None:
    assert conflicts(allocations) == expected
    assert conflicts(allocations, work_budget=None) == expected


@pytest.mark.parametrize(
    ("allocations", "work_budget", "expected"),
    [
        pytest.param((), 1, [], id="empty"),
        pytest.param(OVERLAP, 1, [1, 1, 0], id="input_order"),
        pytest.param(DUPLICATED, 1, [1, 1], id="duplicate_ids"),
        pytest.param(CLIQUE, 1, [3, 3, 3, 3], id="scalar_ignores_the_budget"),
        pytest.param(CLIQUE, None, [3, 3, 3, 3], id="unbounded"),
    ],
)
def test_conflict_degrees(
    allocations: tuple[Allocation, ...], work_budget: int | None, expected: list[int]
) -> None:
    assert conflict_degrees(allocations, work_budget=work_budget) == expected


@pytest.mark.parametrize(
    ("query", "allocations", "kwargs", "error", "match"),
    [
        (conflicts, DUPLICATED, {}, ValueError, "unique"),
        (conflicts, MIXED, {}, ValueError, "dimension"),
        (conflicts, CLIQUE, {"work_budget": 1}, RuntimeError, "work_budget"),
        (conflicts, (), {"work_budget": -1}, ValueError, "must be non-negative"),
        (conflict_degrees, SKEWED, {"work_budget": 1}, RuntimeError, "work_budget"),
        (conflict_degrees, (), {"work_budget": -1}, ValueError, "must be non-negative"),
        (conflict_graph, FIFTY, {"max_entries": 100}, RuntimeError, "neighbor entries"),
        (conflict_graph, FIFTY, {"max_entries": -1}, ValueError, "max_entries"),
    ],
)
def test_conflict_queries_reject(
    query: object,
    allocations: tuple[Allocation, ...],
    kwargs: dict[str, int],
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        query(allocations, **kwargs)


def test_conflict_graph_builds_an_adjacency_inside_the_entry_ceiling() -> None:
    assert conflict_graph(FIFTY, max_entries=50 * 49).pair_count == 50 * 49 // 2


def test_conflict_degrees_tiny_budget_admits_degenerate_clock_columns() -> None:
    lockstep = tuple(
        Allocation(id=i, size=8, start=(i, i), end=(i + 2, i + 2)) for i in range(50)
    )
    assert conflict_degrees(lockstep, work_budget=1) == conflict_degrees(lockstep)


def test_conflict_degrees_default_budget_admits_a_wide_clock_sweep() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(0,) * 64, end=(1,) * 64) for i in range(3000)
    )
    assert set(conflict_degrees(allocations)) == {2999}


def test_conflicts_is_deterministic_under_parallel_fill() -> None:
    rng = Random(9)
    allocations = []
    for i in range(600):
        start = rng.randint(0, 100)
        allocations.append(
            Allocation(id=i, size=8, start=start, end=start + rng.randint(1, 10))
        )
    fixed = tuple(allocations)
    assert conflicts(fixed) == conflicts(fixed)


def _random_instance(rng: Random) -> tuple[Allocation, ...]:
    dim = rng.choice((1, 2, 3))
    allocations = []
    for i in range(rng.randint(1, 12)):
        start = tuple(rng.randint(0, 5) for _ in range(dim))
        delta = [rng.randint(0, 3) for _ in range(dim)]
        if sum(delta) == 0:
            delta[rng.randrange(dim)] = 1
        end = tuple(s + x for s, x in zip(start, delta, strict=True))
        if dim == 1:
            allocations.append(Allocation(id=i, size=8, start=start[0], end=end[0]))
        else:
            allocations.append(Allocation(id=i, size=8, start=start, end=end))
    return tuple(allocations)


def test_conflicts_match_pairwise_overlaps() -> None:
    rng = Random(5)
    for _ in range(100):
        allocations = _random_instance(rng)
        conflict_map = conflicts(allocations)
        for alloc in allocations:
            expected = {
                other.id
                for other in allocations
                if other.id != alloc.id and alloc.conflicts_with(other)
            }
            assert conflict_map[alloc.id] == expected
        degrees = [len(conflict_map[alloc.id]) for alloc in allocations]
        assert conflict_degrees(allocations) == degrees


def test_degrees_match_conflict_map_on_degenerate_clocks() -> None:
    rng = Random(11)
    for _ in range(60):
        base = [
            (i, rng.randint(0, 20), rng.randint(1, 6))
            for i in range(rng.randint(1, 30))
        ]
        shapes = (
            [Allocation(id=i, size=8, start=s, end=s + d) for i, s, d in base],
            [
                Allocation(id=i, size=8, start=(s, 0, 0), end=(s + d, 0, 0))
                for i, s, d in base
            ],
            [
                Allocation(id=i, size=8, start=(s,) * 4, end=(s + d,) * 4)
                for i, s, d in base
            ],
        )
        expected = conflicts(tuple(shapes[0]))
        for shape in shapes:
            allocations = tuple(shape)
            assert conflicts(allocations) == expected
            degrees = conflict_degrees(allocations)
            for alloc, degree in zip(allocations, degrees, strict=True):
                assert degree == len(expected[alloc.id])


def test_columns_agreeing_until_the_last_row_keep_the_exact_relation() -> None:
    # The shape a column-against-column reduction spends O(n * d^2) on: every
    # column matches column 0 until the final row, where each diverges, so
    # none may be dropped and the relation must come back exact.
    rng = Random(7)
    for dim in (2, 5, 16):
        base = [(i, rng.randint(0, 30), rng.randint(1, 5)) for i in range(40)]
        allocations = []
        for index, (i, start, duration) in enumerate(base):
            shift = [c if index == len(base) - 1 else 0 for c in range(dim)]
            allocations.append(
                Allocation(
                    id=i,
                    size=8,
                    start=tuple(start + s for s in shift),
                    end=tuple(start + duration + s for s in shift),
                )
            )
        instance = tuple(allocations)
        expected = [
            sum(1 for other in instance if other.id != a.id and a.conflicts_with(other))
            for a in instance
        ]
        assert conflict_degrees(instance) == expected
