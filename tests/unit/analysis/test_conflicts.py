#
# SPDX-License-Identifier: Apache-2.0
#

from random import Random

import pytest
from omnimalloc.analysis import conflict_degrees, conflicts
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


def test_conflict_degrees_over_budget_raise() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(0, 0), end=(10, 10)) for i in range(4)
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
