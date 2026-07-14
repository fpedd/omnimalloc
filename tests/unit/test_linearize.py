#
# SPDX-License-Identifier: Apache-2.0
#

import random

import pytest
from omnimalloc import try_linearize
from omnimalloc._cpp import compute_temporal_overlaps
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation


def _overlap_map(allocations: tuple[Allocation, ...]) -> dict[object, set[object]]:
    overlaps = compute_temporal_overlaps(allocations)
    return {a.id: set(overlaps.get(a.id, ())) for a in allocations}


def test_scalar_input_returned_unchanged() -> None:
    allocations = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=2, end=6),
    )
    assert try_linearize(allocations) is allocations


def test_empty_input_returned_unchanged() -> None:
    assert try_linearize(()) == ()


def test_ordered_chain_linearizes() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(i, i), end=(i + 1, i + 1)) for i in range(4)
    )
    linearized = try_linearize(allocations)
    assert linearized is not None
    assert all(a.dim == 1 for a in linearized)
    assert _overlap_map(linearized) == _overlap_map(allocations)


def test_concurrent_pair_linearizes_to_overlap() -> None:
    allocations = (
        Allocation(id=1, size=100, start=(0, 5), end=(1, 6)),
        Allocation(id=2, size=100, start=(2, 0), end=(3, 1)),
    )
    linearized = try_linearize(allocations)
    assert linearized is not None
    assert linearized[0].overlaps_temporally(linearized[1])


def test_two_plus_two_returns_none() -> None:
    allocations = (
        Allocation(id="a", size=8, start=(0, 0), end=(1, 0)),
        Allocation(id="b", size=8, start=(1, 0), end=(2, 0)),
        Allocation(id="c", size=8, start=(0, 0), end=(0, 1)),
        Allocation(id="d", size=8, start=(0, 1), end=(0, 2)),
    )
    assert try_linearize(allocations) is None


def test_mixed_dimensions_rejected() -> None:
    mixed = (
        Allocation(id=1, size=8, start=0, end=4),
        Allocation(id=2, size=8, start=(0, 1), end=(2, 2)),
    )
    with pytest.raises(ValueError, match="dimension"):
        try_linearize(mixed)


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


def test_linearize_preserves_conflicts_on_random_interval_orders() -> None:
    rng = random.Random(3)
    for _ in range(20):
        allocs = []
        for i in range(rng.randint(2, 20)):
            size = rng.randint(1, 64)
            start = rng.randint(0, 12)
            allocs.append(
                Allocation(id=i, size=size, start=start, end=start + rng.randint(1, 6))
            )
        scalar = tuple(allocs)
        lockstep = tuple(
            Allocation(
                id=a.id, size=a.size, start=(a.start, a.start), end=(a.end, a.end)
            )
            for a in scalar
        )
        linearized = try_linearize(lockstep)
        assert linearized is not None
        assert _overlap_map(linearized) == _overlap_map(scalar)


def test_linearize_handles_duplicate_clock_values() -> None:
    allocations = (
        Allocation(id=1, size=8, start=(0, 0), end=(2, 1)),
        Allocation(id=2, size=16, start=(0, 0), end=(2, 1)),
        Allocation(id=3, size=32, start=(2, 1), end=(3, 2)),
        Allocation(id=4, size=64, start=(2, 1), end=(3, 2)),
    )
    linearized = try_linearize(allocations)
    assert linearized is not None
    assert _overlap_map(linearized) == _overlap_map(allocations)


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
    assert validate_allocation(Pool(id="p", allocations=placed))
