#
# SPDX-License-Identifier: Apache-2.0
#

from typing import cast

import pytest
from omnimalloc._cpp import Partition, try_solve_many
from omnimalloc.allocators.supermalloc import (
    Heuristic,
    SortKey,
    SupermallocAllocator,
    SupermallocResult,
)
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _assert_valid(result: tuple[Allocation, ...]) -> None:
    validate_allocation(Pool(id="test_pool", allocations=result))


def _ablation_solve(
    allocations: tuple[Allocation, ...],
    canonical: bool = True,
    dominance: bool = True,
    floor_inference: bool = True,
    monotonic_floor: bool = True,
    decompose: bool = True,
) -> tuple[Allocation, ...]:
    partition = Partition.from_allocations(allocations)
    bound = sum(a.size for a in allocations) + 1
    solution = try_solve_many(
        [partition.with_bound(bound)],
        bound,
        None,
        canonical,
        dominance,
        floor_inference,
        monotonic_floor,
        decompose,
        2.0,
        1,
    )
    assert solution is not None
    return tuple(solution.allocations)


def test_empty() -> None:
    assert SupermallocAllocator().allocate(()) == ()


def test_single() -> None:
    result = SupermallocAllocator().allocate(
        (Allocation(id=1, size=100, start=0, end=10),)
    )
    assert result[0].offset == 0


def test_disjoint_share_offset() -> None:
    result = SupermallocAllocator().allocate(
        (
            Allocation(id=1, size=100, start=0, end=10),
            Allocation(id=2, size=200, start=10, end=20),
        )
    )
    assert result[0].offset == 0
    assert result[1].offset == 0


def test_overlapping_reach_lower_bound() -> None:
    result = SupermallocAllocator().allocate(
        (
            Allocation(id=1, size=100, start=0, end=10),
            Allocation(id=2, size=50, start=5, end=15),
            Allocation(id=3, size=25, start=0, end=15),
        )
    )
    _assert_valid(result)
    assert placement_pressure(result) == 175


def test_preserves_ids_and_sizes() -> None:
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=0, end=10) for i in range(5)
    )
    result = SupermallocAllocator().allocate(allocations)
    assert {(a.id, a.size) for a in result} == {(a.id, a.size) for a in allocations}


@pytest.mark.parametrize(
    "flag",
    ["canonical", "dominance", "floor_inference", "monotonic_floor", "decompose"],
)
def test_ablation_flags_still_valid(flag: str) -> None:
    allocations = tuple(
        Allocation(id=i, size=100 + i, start=i % 4, end=4 + i % 5) for i in range(8)
    )
    placed = _ablation_solve(allocations, **{flag: False})
    _assert_valid(placed)


def test_deterministic_single_threaded() -> None:
    allocations = tuple(
        Allocation(id=i, size=50 + 7 * i, start=i % 3, end=5 + i % 4) for i in range(10)
    )
    first = SupermallocAllocator(num_threads=1).allocate(allocations)
    second = SupermallocAllocator(num_threads=1).allocate(allocations)
    assert [(a.id, a.offset) for a in first] == [(a.id, a.offset) for a in second]


def test_custom_heuristics() -> None:
    allocator = SupermallocAllocator(
        heuristics=((SortKey.SIZE,), (SortKey.AREA, SortKey.DURATION))
    )
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=i % 3, end=3 + i % 4)
        for i in range(6)
    )
    result = allocator.allocate(allocations)
    _assert_valid(result)


def test_empty_heuristics_rejected() -> None:
    with pytest.raises(ValueError, match="at least one heuristic"):
        SupermallocAllocator(heuristics=())


def test_invalid_sort_key_rejected() -> None:
    allocator = SupermallocAllocator(
        heuristics=cast("tuple[Heuristic, ...]", (("X",),))
    )
    with pytest.raises(ValueError, match="Unknown sort key"):
        allocator.allocate((Allocation(id=1, size=10, start=0, end=5),))


def test_total_size_overflow_rejected() -> None:
    allocations = (
        Allocation(id=1, size=2**62, start=0, end=5),
        Allocation(id=2, size=2**62, start=0, end=5),
    )
    with pytest.raises(ValueError, match="int64"):
        SupermallocAllocator().allocate(allocations)


def test_perfect_tiling_stack() -> None:
    allocations = tuple(
        Allocation(id=f"{layer}.{col}", size=64, start=4 * col, end=4 * (col + 1))
        for layer in range(4)
        for col in range(4)
    )
    result = SupermallocAllocator().allocate(allocations)
    _assert_valid(result)
    assert placement_pressure(result) == 256


def test_interleaved_lifetimes() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=8),
        Allocation(id=2, size=100, start=2, end=6),
        Allocation(id=3, size=100, start=4, end=10),
        Allocation(id=4, size=100, start=0, end=3),
        Allocation(id=5, size=100, start=7, end=10),
    )
    result = SupermallocAllocator().allocate(allocations)
    _assert_valid(result)
    assert placement_pressure(result) == 300


def test_solve_reports_a_proved_optimum() -> None:
    allocations = tuple(Allocation(id=i, size=8, start=i, end=i + 2) for i in range(12))
    result = SupermallocAllocator(timeout=5.0).solve(allocations)
    assert result.peak == result.lower_bound
    assert result.proved_optimal


def test_solve_reports_an_empty_problem_as_optimal() -> None:
    result = SupermallocAllocator().solve(())
    assert result.allocations == ()
    assert result.proved_optimal


def test_solve_rejects_a_partial_result(monkeypatch: pytest.MonkeyPatch) -> None:
    allocations = tuple(Allocation(id=i, size=10, start=0, end=5) for i in range(3))
    allocator = SupermallocAllocator()
    partial = SupermallocResult(
        allocations=(allocations[0].with_offset(0),),
        peak=10,
        lower_bound=10,
        proved_optimal=True,
    )
    monkeypatch.setattr(allocator, "_solve", lambda _: partial)
    with pytest.raises(ValueError, match="returned a different allocation set"):
        allocator.solve(allocations)


def test_solve_agrees_with_allocate() -> None:
    allocations = tuple(
        Allocation(id=i, size=(i % 5) + 1, start=i % 9, end=i % 9 + 4)
        for i in range(30)
    )
    allocator = SupermallocAllocator(timeout=5.0)
    assert allocator.solve(allocations).allocations == allocator.allocate(allocations)
