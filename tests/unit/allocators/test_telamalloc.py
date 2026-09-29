#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.telamalloc import TelamallocAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation


def test_telamalloc_rejects_negative_backtracks() -> None:
    with pytest.raises(ValueError, match="max_backtracks must be non-negative"):
        TelamallocAllocator(max_backtracks=-1)


def test_telamalloc_reaches_the_lower_bound_on_overlapping_lifetimes() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=50, start=5, end=15),
        Allocation(id=3, size=25, start=0, end=15),
    )
    placed = TelamallocAllocator(timeout=None).allocate(allocations)
    assert placement_pressure(placed) == 175


def test_telamalloc_independent_phases_share_address_space() -> None:
    early = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(3))
    late = tuple(Allocation(id=10 + i, size=100, start=20, end=30) for i in range(3))
    placed = TelamallocAllocator().allocate(early + late)
    assert sorted(a.offset for a in placed[3:]) == [0, 100, 200]


def test_telamalloc_rejects_int64_overflow_inputs() -> None:
    huge = tuple(
        Allocation(id=i, size=(2**63 - 1) // 4, start=0, end=10) for i in range(3)
    )
    with pytest.raises(ValueError, match="int64"):
        TelamallocAllocator().allocate(huge)
