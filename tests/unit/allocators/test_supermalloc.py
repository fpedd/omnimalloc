#
# SPDX-License-Identifier: Apache-2.0
#

from typing import cast

import pytest
from omnimalloc.allocators.greedy_base import peak_memory
from omnimalloc.allocators.supermalloc import (
    Heuristic,
    SortKey,
    SupermallocAllocator,
    SupermallocConfig,
)
from omnimalloc.primitives import Allocation, Pool
from omnimalloc.validate import validate_allocation


def _assert_valid(result: tuple[Allocation, ...]) -> None:
    assert validate_allocation(Pool(id="pool", allocations=result))


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
    assert peak_memory(result) == 175


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
    config = SupermallocConfig(timeout=2.0, **{flag: False})
    allocations = tuple(
        Allocation(id=i, size=100 + i, start=i % 4, end=4 + i % 5) for i in range(8)
    )
    result = SupermallocAllocator(config).allocate(allocations)
    _assert_valid(result)


def test_deterministic_single_threaded() -> None:
    config = SupermallocConfig(cores=1)
    allocations = tuple(
        Allocation(id=i, size=50 + 7 * i, start=i % 3, end=5 + i % 4) for i in range(10)
    )
    first = SupermallocAllocator(config).allocate(allocations)
    second = SupermallocAllocator(config).allocate(allocations)
    assert [(a.id, a.offset) for a in first] == [(a.id, a.offset) for a in second]


def test_custom_heuristics_config() -> None:
    config = SupermallocConfig(
        heuristics=((SortKey.SIZE,), (SortKey.AREA, SortKey.WIDTH))
    )
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=i % 3, end=3 + i % 4)
        for i in range(6)
    )
    result = SupermallocAllocator(config).allocate(allocations)
    _assert_valid(result)


def test_empty_heuristics_rejected() -> None:
    with pytest.raises(ValueError, match="at least one heuristic"):
        SupermallocConfig(heuristics=())


def test_invalid_sort_key_rejected() -> None:
    config = SupermallocConfig(heuristics=cast("tuple[Heuristic, ...]", (("X",),)))
    with pytest.raises(ValueError, match="Unknown sort key"):
        SupermallocAllocator(config).allocate(
            (Allocation(id=1, size=10, start=0, end=5),)
        )


def test_total_size_overflow_rejected() -> None:
    allocations = (
        Allocation(id=1, size=2**62, start=0, end=5),
        Allocation(id=2, size=2**62, start=0, end=5),
    )
    with pytest.raises(OverflowError):
        SupermallocAllocator().allocate(allocations)


def test_perfect_tiling_stack() -> None:
    allocations = tuple(
        Allocation(id=f"{layer}.{col}", size=64, start=4 * col, end=4 * (col + 1))
        for layer in range(4)
        for col in range(4)
    )
    result = SupermallocAllocator().allocate(allocations)
    _assert_valid(result)
    assert peak_memory(result) == 256


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
    assert peak_memory(result) == 300
