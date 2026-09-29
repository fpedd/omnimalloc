#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import allocate, validate_allocation
from omnimalloc.analysis import (
    antichain_pressure,
    conflict_degrees,
    conflict_graph,
    placement_pressure,
    try_linearize,
)
from omnimalloc.primitives import Allocation, Memory, Pool, System

HUGE = 2**62
LARGE = 2**61


def test_a_single_huge_allocation_places() -> None:
    placed = allocate((Allocation(id=1, size=LARGE, start=0, end=1),))
    assert placed[0].offset == 0
    assert placed[0].height == LARGE


def test_two_huge_allocations_are_rejected_before_they_overflow() -> None:
    allocations = (
        Allocation(id=1, size=HUGE, start=0, end=5),
        Allocation(id=2, size=HUGE, start=0, end=5),
    )
    with pytest.raises(ValueError, match="exceeds int64 range"):
        allocate(allocations, "omni")


def test_an_offset_plus_size_beyond_int64_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="exceeds int64 range"):
        Allocation(id=1, size=HUGE, start=0, end=5, offset=2**63 - 1)


def test_huge_clock_components_place_and_validate() -> None:
    allocations = tuple(
        Allocation(id=i, size=8, start=(HUGE - 8 + i,) * 4, end=(HUGE - 4 + i,) * 4)
        for i in range(4)
    )
    placed = allocate(allocations, "omni")
    validate_allocation(placed)


def test_one_allocation_at_a_high_clock_dimension() -> None:
    allocation = Allocation(id=1, size=16, start=(0,) * 64, end=(1,) * 64)
    placed = allocate((allocation,), "omni")
    assert placed[0].offset == 0
    assert conflict_degrees((allocation,)) == [0]
    assert len(conflict_graph((allocation,))) == 1
    assert antichain_pressure((allocation,), work_budget=None) == 16


def test_empty_analysis_inputs_are_defined() -> None:
    assert conflict_degrees(()) == []
    assert antichain_pressure((), work_budget=None) == 0
    assert placement_pressure(()) == 0
    assert try_linearize(()) == ()


def test_a_one_byte_allocation_places_beside_a_large_one() -> None:
    allocations = (
        Allocation(id="tiny", size=1, start=0, end=5),
        Allocation(id="real", size=10, start=0, end=5),
    )
    placed = allocate(allocations, "omni")
    validate_allocation(placed)
    assert placement_pressure(placed) == 11


def test_empty_hierarchy_levels_validate() -> None:
    validate_allocation(System(id="s", memories=()))
    validate_allocation(Memory(id="m", pools=(), size=0))
    validate_allocation(allocate(Memory(id="m", pools=(Pool(id="p", allocations=()),))))
