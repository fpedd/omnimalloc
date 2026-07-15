#
# SPDX-License-Identifier: Apache-2.0
#

import pickle

import pytest
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pressure import get_pressure


def test_vector_creation() -> None:
    alloc = Allocation(id=7, size=64, start=(3, 0), end=(5, 2))
    assert alloc.start == (3, 0)
    assert alloc.end == (5, 2)
    assert alloc.dim == 2


def test_scalar_dim_is_one() -> None:
    assert Allocation(id=1, size=1, start=0, end=1).dim == 1


def test_one_tuple_normalizes_to_scalar() -> None:
    scalar = Allocation(id=1, size=10, start=3, end=7)
    one_tuple = Allocation(id=1, size=10, start=(3,), end=(7,))
    assert one_tuple == scalar
    assert hash(one_tuple) == hash(scalar)
    assert one_tuple.start == 3
    assert one_tuple.dim == 1


def test_negative_component_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative componentwise"):
        Allocation(id=1, size=1, start=(0, -1), end=(1, 1))


def test_end_below_start_componentwise_rejected() -> None:
    with pytest.raises(ValueError, match="componentwise"):
        Allocation(id=1, size=1, start=(2, 0), end=(1, 5))


def test_end_equal_to_start_rejected() -> None:
    with pytest.raises(ValueError, match="at least one component"):
        Allocation(id=1, size=1, start=(2, 1), end=(2, 1))


def test_start_end_dimension_mismatch_rejected() -> None:
    with pytest.raises(ValueError, match="share one clock dimension"):
        Allocation(id=1, size=1, start=(0, 0), end=(1, 1, 1))


def test_empty_time_point_rejected() -> None:
    with pytest.raises(ValueError, match="at least one component"):
        Allocation(id=1, size=1, start=(), end=())


def test_duration_is_max_per_thread_extent() -> None:
    alloc = Allocation(id=1, size=10, start=(3, 0), end=(5, 4))
    assert alloc.duration == 4
    assert alloc.area == 40


def test_scalar_duration_unchanged() -> None:
    assert Allocation(id=1, size=10, start=3, end=7).duration == 4


def test_no_overlap_when_ordered() -> None:
    first = Allocation(id=1, size=1, start=(0, 0), end=(2, 1))
    second = Allocation(id=2, size=1, start=(2, 1), end=(3, 2))
    assert not first.overlaps_temporally(second)
    assert not second.overlaps_temporally(first)


def test_overlap_when_concurrent() -> None:
    a = Allocation(id=1, size=1, start=(0, 0), end=(5, 1))
    b = Allocation(id=2, size=1, start=(1, 0), end=(2, 3))
    assert a.overlaps_temporally(b)
    assert b.overlaps_temporally(a)


def test_overlap_dimension_mismatch_rejected() -> None:
    scalar = Allocation(id=1, size=1, start=0, end=1)
    vector = Allocation(id=2, size=1, start=(0, 0), end=(1, 1))
    with pytest.raises(ValueError, match="dimension mismatch"):
        scalar.overlaps_temporally(vector)


def test_overlaps_combines_temporal_and_spatial() -> None:
    a = Allocation(id=1, size=100, start=(0, 0), end=(5, 1), offset=0)
    b = Allocation(id=2, size=100, start=(1, 0), end=(2, 3), offset=50)
    c = Allocation(id=3, size=100, start=(1, 0), end=(2, 3), offset=100)
    assert a.overlaps(b)
    assert not a.overlaps(c)


def test_with_offset_preserves_vector_time() -> None:
    alloc = Allocation(id=1, size=8, start=(1, 0), end=(2, 2)).with_offset(16)
    assert alloc.start == (1, 0)
    assert alloc.end == (2, 2)
    assert alloc.offset == 16


def test_repr_shows_tuples() -> None:
    alloc = Allocation(id=1, size=1, start=(3, 0), end=(5, 2))
    assert "start=(3, 0)" in repr(alloc)
    assert "end=(5, 2)" in repr(alloc)


def test_pickle_roundtrip_vector() -> None:
    alloc = Allocation(id="x", size=10, start=(0, 3), end=(5, 4), offset=3)
    restored = pickle.loads(pickle.dumps(alloc))  # noqa: S301
    assert restored == alloc
    assert hash(restored) == hash(alloc)
    assert restored.start == (0, 3)
    assert isinstance(restored.start, tuple)


def test_pressure_supports_vector_time() -> None:
    allocs = (Allocation(id=1, size=1, start=(0, 0), end=(1, 1)),)
    assert get_pressure(allocs) == 1


def test_conflict_without_per_thread_overlap() -> None:
    a = Allocation(id="a", size=1, start=(0, 5), end=(1, 6))
    b = Allocation(id="b", size=1, start=(2, 0), end=(3, 1))
    assert a.overlaps_temporally(b)
    assert b.overlaps_temporally(a)


def test_before_relation_is_transitive_chain() -> None:
    chain = tuple(
        Allocation(id=i, size=1, start=(i, 2 * i), end=(i + 1, 2 * i + 1))
        for i in range(5)
    )
    for i, earlier in enumerate(chain):
        for later in chain[i + 1 :]:
            assert not earlier.overlaps_temporally(later)
            assert not later.overlaps_temporally(earlier)
