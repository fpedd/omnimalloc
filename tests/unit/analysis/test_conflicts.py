#
# SPDX-License-Identifier: Apache-2.0
#

from random import Random

import pytest
from omnimalloc.analysis import conflict_degrees, conflict_graph, conflicts
from omnimalloc.primitives import Allocation


def test_conflicts_empty() -> None:
    assert conflicts(()) == {}


def test_conflicts_scalar_overlap() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=2, end=6),
        Allocation(id=3, size=8, start=6, end=8),
    )
    assert conflicts(allocations) == {1: {2}, 2: {1}, 3: set()}


def test_conflicts_touching_intervals_do_not_conflict() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=4, end=6),
    )
    assert conflicts(allocations) == {1: set(), 2: set()}


def test_conflicts_vector_concurrent() -> None:
    allocations = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=8, start=(0, 0), end=(0, 1)),
    )
    assert conflicts(allocations) == {"a": {"b"}, "b": {"a"}}


def test_conflicts_vector_ordered_do_not_conflict() -> None:
    allocations = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=8, start=(1, 0), end=(2, 0)),
    )
    assert conflicts(allocations) == {"a": set(), "b": set()}


def test_conflicts_rejects_duplicate_ids() -> None:
    duplicated = (
        Allocation(id=1, size=8, start=0, end=2),
        Allocation(id=1, size=8, start=1, end=3),
    )
    with pytest.raises(ValueError, match="unique"):
        conflicts(duplicated)


def test_conflicts_rejects_mixed_dimensions() -> None:
    mixed = (
        Allocation(id=1, size=8, start=0, end=1),
        Allocation(id=2, size=8, start=(0, 0), end=(1, 1)),
    )
    with pytest.raises(ValueError, match="dimension"):
        conflicts(mixed)


def test_conflicts_over_budget_raise() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=10) for i in range(4))
    with pytest.raises(RuntimeError, match="work_budget"):
        conflicts(allocations, work_budget=1)


def test_conflicts_unbounded_budget_always_computes() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=10) for i in range(2))
    assert conflicts(allocations, work_budget=None) == {0: {1}, 1: {0}}


def test_conflicts_reject_negative_budget() -> None:
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        conflicts((), work_budget=-1)


def test_conflict_degrees_empty() -> None:
    assert conflict_degrees(()) == []


def test_conflict_degrees_align_with_input_order() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=2, end=6),
        Allocation(id=3, size=8, start=6, end=8),
    )
    assert conflict_degrees(allocations) == [1, 1, 0]


def test_conflict_degrees_allow_duplicate_ids() -> None:
    duplicated = (
        Allocation(id=1, size=8, start=0, end=2),
        Allocation(id=1, size=8, start=1, end=3),
    )
    assert conflict_degrees(duplicated) == [1, 1]


def test_conflict_degrees_tiny_budget_admits_degenerate_clock_columns() -> None:
    lockstep = tuple(
        Allocation(id=i, size=8, start=(i, i), end=(i + 2, i + 2)) for i in range(50)
    )
    assert conflict_degrees(lockstep, work_budget=1) == conflict_degrees(lockstep)


def test_conflict_degrees_over_budget_raise() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(i, 2 * i), end=(i + 10, 2 * i + 10))
        for i in range(4)
    )
    with pytest.raises(RuntimeError, match="work_budget"):
        conflict_degrees(allocations, work_budget=1)


def test_conflict_degrees_scalar_ignores_budget() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=10) for i in range(4))
    assert conflict_degrees(allocations, work_budget=1) == [3, 3, 3, 3]


def test_conflict_degrees_unbounded_budget_always_counts() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=10) for i in range(4))
    assert conflict_degrees(allocations, work_budget=None) == [3, 3, 3, 3]


def test_conflict_degrees_reject_negative_budget() -> None:
    with pytest.raises(ValueError, match="work_budget must be non-negative"):
        conflict_degrees((), work_budget=-1)


def test_conflict_degrees_default_budget_admits_a_wide_clock_sweep() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(0,) * 64, end=(1,) * 64) for i in range(3000)
    )
    assert set(conflict_degrees(allocations)) == {2999}


def test_conflicts_and_degrees_agree_on_vector_input() -> None:
    allocations = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=8, start=(0, 0), end=(0, 1)),
        Allocation(id="c", size=8, start=(1, 1), end=(2, 1)),
        Allocation(id="d", size=8, start=(0, 0), end=(2, 2)),
    )
    conflict_map = conflicts(allocations)
    degrees = conflict_degrees(allocations)
    assert [len(conflict_map[a.id]) for a in allocations] == degrees


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


def test_conflicts_ignore_constant_padding_columns() -> None:
    scalar = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=2, end=6),
        Allocation(id=3, size=8, start=6, end=8),
    )
    padded = (
        Allocation(id=1, size=8, start=(0, 0, 0), end=(4, 0, 0)),
        Allocation(id=2, size=8, start=(2, 0, 0), end=(6, 0, 0)),
        Allocation(id=3, size=8, start=(6, 0, 0), end=(8, 0, 0)),
    )
    assert conflicts(padded) == conflicts(scalar)


def test_conflicts_ignore_duplicate_columns() -> None:
    scalar = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=2, end=6),
    )
    lockstep = (
        Allocation(id=1, size=8, start=(0, 0, 0), end=(4, 4, 4)),
        Allocation(id=2, size=8, start=(2, 2, 2), end=(6, 6, 6)),
    )
    assert conflicts(lockstep) == conflicts(scalar)


def test_conflicts_keep_column_pinned_per_row_but_varying_across_rows() -> None:
    allocations = (
        Allocation(id=1, size=8, start=(0, 5), end=(4, 5)),
        Allocation(id=2, size=8, start=(5, 0), end=(9, 0)),
    )
    assert conflicts(allocations) == {1: {2}, 2: {1}}


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


def test_conflict_graph_refuses_an_adjacency_over_the_entry_ceiling() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=1) for i in range(50))
    with pytest.raises(RuntimeError, match="neighbor entries"):
        conflict_graph(allocations, max_entries=100)


def test_conflict_graph_builds_an_adjacency_inside_the_entry_ceiling() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=0, end=1) for i in range(50))
    graph = conflict_graph(allocations, max_entries=50 * 49)
    assert graph.pair_count == 50 * 49 // 2


def test_conflict_graph_rejects_a_negative_entry_ceiling() -> None:
    allocations = (Allocation(id=1, size=8, start=0, end=1),)
    with pytest.raises(ValueError, match="max_entries"):
        conflict_graph(allocations, max_entries=-1)


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
