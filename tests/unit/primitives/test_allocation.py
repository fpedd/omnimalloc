#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import Allocation, BufferKind


def test_basic_creation_with_int_id_simple() -> None:
    """Test creating an allocation with int id."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.id == 1
    assert alloc.size == 100
    assert alloc.start == 0
    assert alloc.end == 10
    assert alloc.offset is None
    assert alloc.kind is None


def test_basic_creation_with_int_id() -> None:
    """Test creating an allocation with integer id."""
    alloc = Allocation(id=42, size=100, start=0, end=10)
    assert alloc.id == 42
    assert alloc.size == 100
    assert alloc.start == 0
    assert alloc.end == 10


def test_basic_creation_with_str_id() -> None:
    """Test creating an allocation with string id."""
    alloc = Allocation(id="alloc_1", size=100, start=0, end=10)
    assert alloc.id == "alloc_1"
    assert alloc.size == 100
    assert alloc.start == 0
    assert alloc.end == 10
    assert alloc.offset is None
    assert alloc.kind is None


def test_creation_with_offset() -> None:
    """Test creating an allocation with offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert alloc.offset == 50
    assert alloc.is_allocated is True


def test_creation_with_kind() -> None:
    """Test creating an allocation with kind."""
    alloc = Allocation(
        id=1,
        size=100,
        start=0,
        end=10,
        kind=BufferKind.WORKSPACE,
    )
    assert alloc.kind == BufferKind.WORKSPACE


def test_negative_start() -> None:
    """Test that negative start raises ValueError."""
    with pytest.raises(ValueError, match="start must be non-negative"):
        Allocation(id=1, size=100, start=-1, end=10)


def test_end_equal_to_start() -> None:
    """Test that end equal to start raises ValueError."""
    with pytest.raises(ValueError, match=r"end .* must be > start"):
        Allocation(id=1, size=100, start=5, end=5)


def test_end_less_than_start() -> None:
    """Test that end less than start raises ValueError."""
    with pytest.raises(ValueError, match=r"end .* must be > start"):
        Allocation(id=1, size=100, start=10, end=5)


def test_zero_size() -> None:
    """Test that zero size raises ValueError."""
    with pytest.raises(ValueError, match="size must be positive"):
        Allocation(id=1, size=0, start=0, end=10)


def test_negative_size() -> None:
    """Test that negative size raises ValueError."""
    with pytest.raises(ValueError, match="size must be positive"):
        Allocation(id=1, size=-100, start=0, end=10)


def test_negative_offset() -> None:
    """Test that negative offset raises ValueError."""
    with pytest.raises(ValueError, match="offset must be non-negative"):
        Allocation(id=1, size=100, start=0, end=10, offset=-1)


def test_zero_offset() -> None:
    """Test that zero offset is valid."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    assert alloc.offset == 0


def test_is_allocated_with_offset() -> None:
    """Test is_allocated returns True when offset is set."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert alloc.is_allocated is True


def test_is_allocated_without_offset() -> None:
    """Test is_allocated returns False when offset is None."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.is_allocated is False


def test_duration() -> None:
    """Test duration calculation."""
    alloc = Allocation(id=1, size=100, start=5, end=15)
    assert alloc.duration == 10


def test_duration_single_timestep() -> None:
    """Test duration for single timestep."""
    alloc = Allocation(id=1, size=100, start=5, end=6)
    assert alloc.duration == 1


def test_height_with_offset() -> None:
    """Test height calculation with offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert alloc.height == 150


def test_height_without_offset() -> None:
    """Test height returns None without offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.height is None


def test_height_with_zero_offset() -> None:
    """Test height calculation with zero offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    assert alloc.height == 100


def test_area() -> None:
    """Test area calculation."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.area == 1000


def test_area_different_values() -> None:
    """Test area calculation with different values."""
    alloc = Allocation(id=1, size=256, start=5, end=20)
    assert alloc.area == 256 * 15


def test_overlaps_temporally_partial_overlap() -> None:
    """Test temporal overlap with partial overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert alloc1.overlaps_temporally(alloc2)
    assert alloc2.overlaps_temporally(alloc1)


def test_overlaps_temporally_complete_overlap() -> None:
    """Test temporal overlap when one contains the other."""
    alloc1 = Allocation(id=101, size=100, start=0, end=20)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert alloc1.overlaps_temporally(alloc2)
    assert alloc2.overlaps_temporally(alloc1)


def test_overlaps_temporally_exact_match() -> None:
    """Test temporal overlap with exact same time range."""
    alloc1 = Allocation(id=101, size=100, start=5, end=15)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert alloc1.overlaps_temporally(alloc2)
    assert alloc2.overlaps_temporally(alloc1)


def test_no_temporal_overlap_adjacent() -> None:
    """Test no temporal overlap when adjacent."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=10, end=20)
    assert not alloc1.overlaps_temporally(alloc2)
    assert not alloc2.overlaps_temporally(alloc1)


def test_no_temporal_overlap_separated() -> None:
    """Test no temporal overlap when separated."""
    alloc1 = Allocation(id=101, size=100, start=0, end=5)
    alloc2 = Allocation(id=102, size=100, start=10, end=15)
    assert not alloc1.overlaps_temporally(alloc2)
    assert not alloc2.overlaps_temporally(alloc1)


def test_temporal_overlap_single_timestep() -> None:
    """Test temporal overlap with single timestep overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=9, end=20)
    assert alloc1.overlaps_temporally(alloc2)
    assert alloc2.overlaps_temporally(alloc1)


def test_overlaps_spatially_partial_overlap() -> None:
    """Test spatial overlap with partial overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=50)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_overlaps_spatially_complete_overlap() -> None:
    """Test spatial overlap when one contains the other."""
    alloc1 = Allocation(id=101, size=200, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=50)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_overlaps_spatially_exact_match() -> None:
    """Test spatial overlap with exact same memory range."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=50)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=50)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_adjacent() -> None:
    """Test no spatial overlap when adjacent in memory."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=100)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_separated() -> None:
    """Test no spatial overlap when separated in memory."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=200)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_without_offset_first() -> None:
    """Test no spatial overlap when first allocation has no offset."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_without_offset_second() -> None:
    """Test no spatial overlap when second allocation has no offset."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_both_without_offset() -> None:
    """Test no spatial overlap when both allocations have no offset."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=0, end=10)
    assert not alloc1.overlaps_spatially(alloc2)


def test_spatial_overlap_single_byte() -> None:
    """Test spatial overlap with single byte overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=99)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_overlaps_both_temporal_and_spatial() -> None:
    """Test overlap when both temporal and spatial overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=5, end=15, offset=50)
    assert alloc1.overlaps(alloc2)
    assert alloc2.overlaps(alloc1)


def test_no_overlaps_temporal_only() -> None:
    """Test no overlap when only temporal overlap exists."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=5, end=15, offset=200)
    assert not alloc1.overlaps(alloc2)
    assert not alloc2.overlaps(alloc1)


def test_no_overlaps_spatial_only() -> None:
    """Test no overlap when only spatial overlap exists."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=20, end=30, offset=50)
    assert not alloc1.overlaps(alloc2)
    assert not alloc2.overlaps(alloc1)


def test_no_overlaps_neither() -> None:
    """Test no overlap when neither temporal nor spatial overlap."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=20, end=30, offset=200)
    assert not alloc1.overlaps(alloc2)
    assert not alloc2.overlaps(alloc1)


def test_no_overlaps_without_offset() -> None:
    """Test no overlap when allocations don't have offsets."""
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert not alloc1.overlaps(alloc2)


def test_overlaps_exact_match() -> None:
    """Test overlap with exact same time and memory range."""
    alloc1 = Allocation(id=101, size=100, start=5, end=15, offset=50)
    alloc2 = Allocation(id=102, size=100, start=5, end=15, offset=50)
    assert alloc1.overlaps(alloc2)
    assert alloc2.overlaps(alloc1)


def test_with_offset_from_none() -> None:
    """Test setting offset on allocation without offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    new_alloc = alloc.with_offset(50)
    assert new_alloc.offset == 50
    assert new_alloc.id == alloc.id
    assert new_alloc.size == alloc.size
    assert new_alloc.start == alloc.start
    assert new_alloc.end == alloc.end
    assert new_alloc.kind == alloc.kind
    assert alloc.offset is None


def test_with_offset_replace_existing() -> None:
    """Test replacing existing offset."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    new_alloc = alloc.with_offset(100)
    assert new_alloc.offset == 100
    assert alloc.offset == 50


def test_with_offset_zero() -> None:
    """Test setting offset to zero."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    new_alloc = alloc.with_offset(0)
    assert new_alloc.offset == 0


def test_with_offset_preserves_kind() -> None:
    """Test that with_offset preserves kind."""
    alloc = Allocation(
        id=1,
        size=100,
        start=0,
        end=10,
        kind=BufferKind.CONSTANT,
    )
    new_alloc = alloc.with_offset(50)
    assert new_alloc.kind == BufferKind.CONSTANT


def test_with_offset_immutability() -> None:
    """Test that original allocation is not modified."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    new_alloc = alloc.with_offset(50)
    assert alloc is not new_alloc
    assert alloc.offset is None
    assert new_alloc.offset == 50


def test_cannot_modify_id() -> None:
    """Test that id cannot be modified."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    with pytest.raises(AttributeError):
        alloc.id = "new_id"  # type: ignore[misc]


def test_cannot_modify_size() -> None:
    """Test that size cannot be modified."""
    alloc = Allocation(id=1, size=100, start=0, end=10)
    with pytest.raises(AttributeError):
        alloc.size = 200  # type: ignore[misc]


def test_cannot_modify_offset() -> None:
    """Test that offset cannot be modified."""
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    with pytest.raises(AttributeError):
        alloc.offset = 100  # type: ignore[misc]


def test_large_values() -> None:
    """Test allocation with large values."""
    alloc = Allocation(
        id=999,
        size=10**12,
        start=0,
        end=10**6,
        offset=10**15,
    )
    assert alloc.size == 10**12
    assert alloc.height == 10**15 + 10**12
    assert alloc.area == 10**12 * 10**6
