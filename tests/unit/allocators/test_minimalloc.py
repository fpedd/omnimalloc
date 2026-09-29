#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.minimalloc import HAS_MINIMALLOC, MinimallocAllocator
from omnimalloc.analysis import placement_pressure
from omnimalloc.primitives import Allocation

pytestmark = pytest.mark.skipif(not HAS_MINIMALLOC, reason="minimalloc not installed")


def test_minimalloc_preserves_id_types() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id="two", size=50, start=5, end=15),
    )
    placed = MinimallocAllocator().allocate(allocations)
    assert [a.id for a in placed] == [1, "two"]


def test_minimalloc_finds_optimal_packing() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=5),
        Allocation(id=2, size=100, start=3, end=8),
        Allocation(id=3, size=100, start=6, end=10),
        Allocation(id=4, size=50, start=0, end=10),
    )
    assert placement_pressure(MinimallocAllocator().allocate(allocations)) == 250
