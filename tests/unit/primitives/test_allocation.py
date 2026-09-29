#
# SPDX-License-Identifier: Apache-2.0
#

import pickle

import pytest
from omnimalloc.primitives import Allocation, AllocationKind


@pytest.mark.parametrize("id_", [1, "alloc_1"])
def test_creation_defaults(id_: int | str) -> None:
    alloc = Allocation(id=id_, size=100, start=0, end=10)
    assert (alloc.id, alloc.size, alloc.start, alloc.end) == (id_, 100, 0, 10)
    assert alloc.offset is None
    assert alloc.kind is None
    assert alloc.is_allocated is False
    assert alloc.height is None


def test_creation_with_offset_and_kind() -> None:
    alloc = Allocation(
        id=1, size=100, start=0, end=10, offset=50, kind=AllocationKind.WORKSPACE
    )
    assert alloc.offset == 50
    assert alloc.kind == AllocationKind.WORKSPACE
    assert alloc.is_allocated is True


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"start": -1}, "start must be non-negative"),
        ({"start": 5, "end": 5}, r"end .* must be > start"),
        ({"start": 10, "end": 5}, r"end .* must be > start"),
        ({"size": 0}, "size must be positive"),
        ({"size": -100}, "size must be positive"),
        ({"offset": -1}, "offset must be non-negative"),
        ({"size": 2**62, "offset": 2**62}, "exceeds int64"),
    ],
)
def test_construction_errors(kwargs: dict[str, int], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        Allocation(**{"id": 1, "size": 100, "start": 0, "end": 10, **kwargs})


@pytest.mark.parametrize(
    ("kwargs", "duration", "height", "area"),
    [
        ({"start": 5, "end": 15}, 10, None, 1000),
        ({"start": 5, "end": 6}, 1, None, 100),
        ({"offset": 0}, 10, 100, 1000),
        ({"offset": 50}, 10, 150, 1000),
        ({"size": 256, "start": 5, "end": 20}, 15, None, 256 * 15),
        ({"size": 1, "offset": 2**63 - 2}, 10, 2**63 - 1, 10),
        (
            {"size": 10**12, "end": 10**6, "offset": 10**15},
            10**6,
            10**15 + 10**12,
            10**18,
        ),
    ],
)
def test_derived_properties(
    kwargs: dict[str, int], duration: int, height: int | None, area: int
) -> None:
    alloc = Allocation(**{"id": 1, "size": 100, "start": 0, "end": 10, **kwargs})
    assert alloc.duration == duration
    assert alloc.height == height
    assert alloc.area == area


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ((0, 10), (5, 15), True),
        ((0, 20), (5, 15), True),
        ((5, 15), (5, 15), True),
        ((0, 10), (9, 20), True),
        ((0, 10), (10, 20), False),
        ((0, 5), (10, 15), False),
    ],
)
def test_conflicts_with(
    first: tuple[int, int], second: tuple[int, int], expected: bool
) -> None:
    a = Allocation(id=1, size=100, start=first[0], end=first[1])
    b = Allocation(id=2, size=100, start=second[0], end=second[1])
    assert a.conflicts_with(b) is expected
    assert b.conflicts_with(a) is expected


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ((100, 0), (100, 50), True),
        ((200, 0), (50, 50), True),
        ((100, 50), (100, 50), True),
        ((100, 0), (100, 99), True),
        ((100, 0), (100, 100), False),
        ((100, 0), (100, 200), False),
        ((100, None), (100, 0), False),
        ((100, None), (100, None), False),
    ],
)
def test_overlaps_spatially(
    first: tuple[int, int | None], second: tuple[int, int | None], expected: bool
) -> None:
    a = Allocation(id=1, size=first[0], start=0, end=10, offset=first[1])
    b = Allocation(id=2, size=second[0], start=0, end=10, offset=second[1])
    assert a.overlaps_spatially(b) is expected
    assert b.overlaps_spatially(a) is expected


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ((0, 10, 0), (5, 15, 50), True),
        ((5, 15, 50), (5, 15, 50), True),
        ((0, 10, 0), (5, 15, 200), False),
        ((0, 10, 0), (20, 30, 50), False),
        ((0, 10, 0), (20, 30, 200), False),
        ((0, 10, None), (5, 15, None), False),
    ],
)
def test_overlaps(
    first: tuple[int, int, int | None],
    second: tuple[int, int, int | None],
    expected: bool,
) -> None:
    a = Allocation(id=1, size=100, start=first[0], end=first[1], offset=first[2])
    b = Allocation(id=2, size=100, start=second[0], end=second[1], offset=second[2])
    assert a.overlaps(b) is expected
    assert b.overlaps(a) is expected


@pytest.mark.parametrize("old_offset", [None, 50])
@pytest.mark.parametrize("new_offset", [0, 100])
def test_with_offset(old_offset: int | None, new_offset: int) -> None:
    alloc = Allocation(
        id=1, size=100, start=0, end=10, offset=old_offset, kind=AllocationKind.CONSTANT
    )
    moved = alloc.with_offset(new_offset)
    assert moved.offset == new_offset
    assert (moved.id, moved.size, moved.start, moved.end, moved.kind) == (
        1,
        100,
        0,
        10,
        AllocationKind.CONSTANT,
    )
    assert alloc.offset == old_offset


@pytest.mark.parametrize("field", ["id", "size", "offset"])
def test_fields_are_read_only(field: str) -> None:
    alloc = Allocation(id=1, size=100, start=0, end=10, offset=50)
    with pytest.raises(AttributeError):
        setattr(alloc, field, 7)


@pytest.mark.parametrize(
    "alloc",
    [
        Allocation(id="x", size=1, start=0, end=5, offset=3, kind=AllocationKind.INPUT),
        Allocation(id=7, size=10, start=0, end=5),
    ],
)
def test_pickle_roundtrip(alloc: Allocation) -> None:
    restored = pickle.loads(pickle.dumps(alloc))  # noqa: S301
    assert restored == alloc
    assert hash(restored) == hash(alloc)
    assert (restored.id, restored.offset, restored.kind) == (
        alloc.id,
        alloc.offset,
        alloc.kind,
    )


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
