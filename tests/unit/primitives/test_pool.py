#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.greedy import GreedyBySizeAllocator
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.pool import Pool


def _placed(*spans: tuple[int, int | None]) -> tuple[Allocation, ...]:
    """Allocations sharing one lifetime, as (size, offset) pairs."""
    return tuple(
        Allocation(id=i, size=size, start=0, end=10, offset=offset)
        for i, (size, offset) in enumerate(spans)
    )


@pytest.mark.parametrize("id_", [201, "pool_main"])
def test_creation_defaults(id_: int | str) -> None:
    pool = Pool(id=id_, allocations=_placed((100, 0)))
    assert pool.id == id_
    assert len(pool.allocations) == 1
    assert pool.offset is None


@pytest.mark.parametrize("offset", [0, 50, 10**15])
def test_creation_with_offset(offset: int) -> None:
    assert Pool(id=1, allocations=(), offset=offset).offset == offset


@pytest.mark.parametrize(
    ("allocations", "offset", "error", "match"),
    [
        ((), -1, ValueError, "offset must be non-negative"),
        (_placed((100, 0)) * 2, None, ValueError, "allocation ids must be unique"),
        ([1, 2], None, TypeError, "Expected Allocation"),
        ("abc", None, TypeError, "Unsupported entity type"),
    ],
)
def test_construction_errors(
    allocations: object, offset: int | None, error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        Pool(id=1, allocations=allocations, offset=offset)


def test_empty_pool() -> None:
    pool = Pool(id=1, allocations=())
    assert pool.size == 0
    assert pool.pressure == 0
    assert pool.efficiency == 1.0
    assert pool.is_allocated is True
    assert pool.any_allocated is False


@pytest.mark.parametrize(
    ("spans", "size"),
    [
        (((100, 0),), 100),
        (((100, 0), (50, 100)), 150),
        (((100, 0), (100, 50)), 150),
        (((100, 0), (50, 25)), 100),
        (((100, 0), (50, 200)), 250),
        (((100, 1000),), 1100),
        (((10**12, 0), (10**11, 10**12)), 10**12 + 10**11),
    ],
)
def test_size(spans: tuple[tuple[int, int], ...], size: int) -> None:
    assert Pool(id=1, allocations=_placed(*spans)).size == size


@pytest.mark.parametrize("spans", [((100, None), (50, None)), ((100, 0), (50, None))])
def test_size_and_efficiency_of_unallocated_pool_raise(
    spans: tuple[tuple[int, int | None], ...],
) -> None:
    pool = Pool(id=1, allocations=_placed(*spans))
    with pytest.raises(ValueError, match="cannot compute size of unallocated pool"):
        _ = pool.size
    with pytest.raises(ValueError, match="efficiency of unallocated pool"):
        _ = pool.efficiency


@pytest.mark.parametrize(
    ("lifetimes", "pressure"),
    [
        (((100, 0, 10),), 100),
        (((100, 0, 10), (50, 0, 10), (75, 0, 10)), 225),
        (((100, 0, 5), (50, 5, 10), (75, 10, 15)), 100),
        (((100, 0, 10), (50, 5, 15), (75, 10, 20)), 150),
    ],
)
def test_pressure(lifetimes: tuple[tuple[int, int, int], ...], pressure: int) -> None:
    allocations = tuple(
        Allocation(id=i, size=size, start=start, end=end)
        for i, (size, start, end) in enumerate(lifetimes)
    )
    assert Pool(id=1, allocations=allocations).pressure == pressure


def test_efficiency_is_pressure_over_size() -> None:
    pool = Pool(id=1, allocations=_placed((100, 0), (50, 150)))
    assert pool.efficiency == 150 / 200


@pytest.mark.parametrize(
    ("spans", "is_allocated", "any_allocated"),
    [
        (((100, 0), (50, 100)), True, True),
        (((100, None), (50, None)), False, False),
        (((100, 0), (50, None)), False, True),
    ],
)
def test_allocation_status(
    spans: tuple[tuple[int, int | None], ...], is_allocated: bool, any_allocated: bool
) -> None:
    pool = Pool(id=1, allocations=_placed(*spans))
    assert pool.is_allocated is is_allocated
    assert pool.any_allocated is any_allocated


def test_with_allocations_keeps_id_and_offset() -> None:
    pool = Pool(id=201, allocations=_placed((100, 0)), offset=50)
    replaced = pool.with_allocations(())
    assert (replaced.id, replaced.offset, replaced.allocations) == (201, 50, ())
    assert len(pool.allocations) == 1


@pytest.mark.parametrize("field", ["id", "allocations", "offset"])
def test_fields_are_read_only(field: str) -> None:
    pool = Pool(id=201, allocations=(), offset=50)
    with pytest.raises(AttributeError):
        setattr(pool, field, 7)


def test_allocate_with_allocator() -> None:
    pool = Pool(id=201, allocations=_placed((100, None), (50, None)))
    allocated = pool.allocate(NaiveAllocator())
    assert allocated.is_allocated is True
    assert (allocated.id, allocated.offset) == (pool.id, pool.offset)
    assert pool.is_allocated is False


def test_allocate_preserves_allocation_order() -> None:
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=0, end=5) for i in range(8)
    )
    allocated = Pool(id=1, allocations=allocations).allocate(GreedyBySizeAllocator())
    assert [a.id for a in allocated.allocations] == list(range(8))


def test_from_allocations_wraps_sequence() -> None:
    allocations = [Allocation(id=1, size=10, start=0, end=5)]
    assert Pool.from_allocations(allocations).allocations == tuple(allocations)


def test_pool_coerces_a_list_of_allocations_to_a_tuple() -> None:
    pool = Pool(id="p", allocations=[Allocation(id=1, size=10, start=0, end=5)])
    assert isinstance(pool.allocations, tuple)
    assert hash(pool)
