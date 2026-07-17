#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.minimalloc import HAS_MINIMALLOC, MinimallocAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool
from omnimalloc.validate import validate_allocation

pytestmark = pytest.mark.skipif(not HAS_MINIMALLOC, reason="minimalloc not installed")


def test_minimalloc_empty() -> None:
    result = MinimallocAllocator().allocate(())
    assert len(result) == 0


def test_minimalloc_single() -> None:
    result = MinimallocAllocator().allocate(
        (Allocation(id=1, size=100, start=0, end=10),)
    )
    assert len(result) == 1
    assert result[0].offset == 0


def test_minimalloc_preserves_id_types() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id="two", size=50, start=5, end=15),
    )
    result = MinimallocAllocator().allocate(allocs)
    assert {a.id for a in result} == {1, "two"}


def test_minimalloc_rejects_duplicate_ids() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=1, size=50, start=5, end=15),
    )
    with pytest.raises(ValueError, match="allocation ids must be unique"):
        MinimallocAllocator().allocate(allocs)


def test_minimalloc_no_temporal_overlap_shares_offset() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=100, start=10, end=20),
    )
    result = MinimallocAllocator().allocate(allocs)
    assert placement_pressure(result) == 100


def test_minimalloc_produces_valid_allocation() -> None:
    allocs = tuple(
        Allocation(id=i, size=(i % 5 + 1) * 64, start=i % 4, end=i % 4 + i % 3 + 1)
        for i in range(20)
    )
    result = MinimallocAllocator().allocate(allocs)
    validate_allocation(Pool(id="test_pool", allocations=result))
    assert {a.id for a in result} == {a.id for a in allocs}


def test_minimalloc_finds_optimal_packing() -> None:
    allocs = (
        Allocation(id=1, size=100, start=0, end=5),
        Allocation(id=2, size=100, start=3, end=8),
        Allocation(id=3, size=100, start=6, end=10),
        Allocation(id=4, size=50, start=0, end=10),
    )
    result = MinimallocAllocator().allocate(allocs)
    validate_allocation(Pool(id="test_pool", allocations=result))
    assert placement_pressure(result) == 250
