#
# SPDX-License-Identifier: Apache-2.0
#

import pickle

import pytest
from omnimalloc.primitives import Allocation, AllocationKind


def test_basic_creation_with_int_id() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.id == 1
    assert alloc.size == 100
    assert alloc.start == 0
    assert alloc.end == 10
    assert alloc.offset is None
    assert alloc.kind is None


def test_basic_creation_with_str_id() -> None:
    alloc = Allocation(id="alloc_1", size=100, start=0, end=10)
    assert alloc.id == "alloc_1"
    assert alloc.size == 100
    assert alloc.start == 0
    assert alloc.end == 10
    assert alloc.offset is None
    assert alloc.kind is None


def test_creation_with_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert alloc.offset == 50
    assert alloc.is_allocated is True


def test_creation_with_kind() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, kind=AllocationKind.WORKSPACE)
    assert alloc.kind == AllocationKind.WORKSPACE


def test_negative_start() -> None:
    with pytest.raises(ValueError, match="start must be non-negative"):
        Allocation(id=1, size=100, start=-1, end=10)


def test_end_equal_to_start() -> None:
    with pytest.raises(ValueError, match=r"end .* must be > start"):
        Allocation(id=1, size=100, start=5, end=5)


def test_end_less_than_start() -> None:
    with pytest.raises(ValueError, match=r"end .* must be > start"):
        Allocation(id=1, size=100, start=10, end=5)


def test_zero_size() -> None:
    with pytest.raises(ValueError, match="size must be positive"):
        Allocation(id=1, size=0, start=0, end=10)


def test_negative_size() -> None:
    with pytest.raises(ValueError, match="size must be positive"):
        Allocation(id=1, size=-100, start=0, end=10)


def test_negative_offset() -> None:
    with pytest.raises(ValueError, match="offset must be non-negative"):
        Allocation(id=1, size=100, start=0, end=10, offset=-1)


def test_zero_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    assert alloc.offset == 0


def test_offset_plus_size_overflow_rejected() -> None:
    with pytest.raises(ValueError, match="exceeds int64"):
        Allocation(id=1, size=2**62, start=0, end=10, offset=2**62)


def test_offset_plus_size_at_int64_max_is_valid() -> None:
    alloc = Allocation(id=1, size=1, start=0, end=10, offset=2**63 - 2)
    assert alloc.height == 2**63 - 1


def test_is_allocated_with_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert alloc.is_allocated is True


def test_is_allocated_without_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.is_allocated is False


def test_duration() -> None:
    alloc = Allocation(id=1, size=100, start=5, end=15)
    assert alloc.duration == 10


def test_duration_single_timestep() -> None:
    alloc = Allocation(id=1, size=100, start=5, end=6)
    assert alloc.duration == 1


def test_height_with_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert alloc.height == 150


def test_height_without_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.height is None


def test_height_with_zero_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=0)
    assert alloc.height == 100


def test_area() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    assert alloc.area == 1000


def test_area_different_values() -> None:
    alloc = Allocation(id=1, size=256, start=5, end=20)
    assert alloc.area == 256 * 15


def test_conflicts_with_partial_overlap() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert alloc1.conflicts_with(alloc2)
    assert alloc2.conflicts_with(alloc1)


def test_conflicts_with_contained_lifetime() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=20)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert alloc1.conflicts_with(alloc2)
    assert alloc2.conflicts_with(alloc1)


def test_conflicts_with_exact_match() -> None:
    alloc1 = Allocation(id=101, size=100, start=5, end=15)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert alloc1.conflicts_with(alloc2)
    assert alloc2.conflicts_with(alloc1)


def test_no_conflict_when_adjacent() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=10, end=20)
    assert not alloc1.conflicts_with(alloc2)
    assert not alloc2.conflicts_with(alloc1)


def test_no_conflict_when_separated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=5)
    alloc2 = Allocation(id=102, size=100, start=10, end=15)
    assert not alloc1.conflicts_with(alloc2)
    assert not alloc2.conflicts_with(alloc1)


def test_conflicts_with_single_timestep() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=9, end=20)
    assert alloc1.conflicts_with(alloc2)
    assert alloc2.conflicts_with(alloc1)


def test_overlaps_spatially_partial_overlap() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=50)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_overlaps_spatially_complete_overlap() -> None:
    alloc1 = Allocation(id=101, size=200, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=50, start=0, end=10, offset=50)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_overlaps_spatially_exact_match() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=50)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=50)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_adjacent() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=100)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_separated() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=200)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_without_offset_first() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=0)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_without_offset_second() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10)
    assert not alloc1.overlaps_spatially(alloc2)
    assert not alloc2.overlaps_spatially(alloc1)


def test_no_spatial_overlap_both_without_offset() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=0, end=10)
    assert not alloc1.overlaps_spatially(alloc2)


def test_spatial_overlap_single_byte() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=0, end=10, offset=99)
    assert alloc1.overlaps_spatially(alloc2)
    assert alloc2.overlaps_spatially(alloc1)


def test_overlaps_both_temporal_and_spatial() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=5, end=15, offset=50)
    assert alloc1.overlaps(alloc2)
    assert alloc2.overlaps(alloc1)


def test_no_overlaps_temporal_only() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=5, end=15, offset=200)
    assert not alloc1.overlaps(alloc2)
    assert not alloc2.overlaps(alloc1)


def test_no_overlaps_spatial_only() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=20, end=30, offset=50)
    assert not alloc1.overlaps(alloc2)
    assert not alloc2.overlaps(alloc1)


def test_no_overlaps_neither() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10, offset=0)
    alloc2 = Allocation(id=102, size=100, start=20, end=30, offset=200)
    assert not alloc1.overlaps(alloc2)
    assert not alloc2.overlaps(alloc1)


def test_no_overlaps_without_offset() -> None:
    alloc1 = Allocation(id=101, size=100, start=0, end=10)
    alloc2 = Allocation(id=102, size=100, start=5, end=15)
    assert not alloc1.overlaps(alloc2)


def test_overlaps_exact_match() -> None:
    alloc1 = Allocation(id=101, size=100, start=5, end=15, offset=50)
    alloc2 = Allocation(id=102, size=100, start=5, end=15, offset=50)
    assert alloc1.overlaps(alloc2)
    assert alloc2.overlaps(alloc1)


def test_with_offset_from_none() -> None:
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
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    new_alloc = alloc.with_offset(100)
    assert new_alloc.offset == 100
    assert alloc.offset == 50


def test_with_offset_zero() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    new_alloc = alloc.with_offset(0)
    assert new_alloc.offset == 0


def test_with_offset_preserves_kind() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, kind=AllocationKind.CONSTANT)
    new_alloc = alloc.with_offset(50)
    assert new_alloc.kind == AllocationKind.CONSTANT


def test_cannot_modify_id() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    with pytest.raises(AttributeError):
        alloc.id = "new_id"  # type: ignore[misc]


def test_cannot_modify_size() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10)
    with pytest.raises(AttributeError):
        alloc.size = 200  # type: ignore[misc]


def test_cannot_modify_offset() -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    with pytest.raises(AttributeError):
        alloc.offset = 100  # type: ignore[misc]


def test_large_values() -> None:
    alloc = Allocation(id=999, size=10**12, start=0, end=10**6, offset=10**15)
    assert alloc.size == 10**12
    assert alloc.height == 10**15 + 10**12
    assert alloc.area == 10**12 * 10**6


def test_pickle_roundtrip() -> None:
    allocs = (
        Allocation(
            id="x", size=10, start=0, end=5, offset=3, kind=AllocationKind.INPUT
        ),
        Allocation(id=7, size=10, start=0, end=5),
    )
    for alloc in allocs:
        restored = pickle.loads(pickle.dumps(alloc))  # noqa: S301
        assert restored == alloc
        assert hash(restored) == hash(alloc)
        assert restored.id == alloc.id
        assert restored.offset == alloc.offset
        assert restored.kind == alloc.kind


def test_equal_allocations_hash_equal() -> None:
    first = Allocation(id=1, size=100, start=0, end=10, offset=50)
    second = Allocation(id=1, size=100, start=0, end=10, offset=50)
    assert first == second
    assert hash(first) == hash(second)


def test_int_and_str_ids_are_distinct_and_repr_apart() -> None:
    numeric = Allocation(id=1, size=100, start=0, end=10)
    textual = Allocation(id="1", size=100, start=0, end=10)
    assert numeric != textual
    assert len({numeric, textual}) == 2
    assert repr(numeric) != repr(textual)


def test_repr_quotes_string_ids_and_leaves_numeric_bare() -> None:
    assert "id='buf 0'" in repr(Allocation(id="buf 0", size=1, start=0, end=1))
    assert "id=7," in repr(Allocation(id=7, size=1, start=0, end=1))


def test_allocation_eq_with_non_allocation_returns_false() -> None:
    alloc = Allocation(id=1, size=10, start=0, end=5)
    assert alloc != None  # noqa: E711
    assert alloc != 5
    assert (alloc == "allocation") is False
    assert alloc in [1, "x", alloc]
