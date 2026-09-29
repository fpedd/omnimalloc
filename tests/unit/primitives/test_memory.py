#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.primitives.memory import Memory
from omnimalloc.primitives.pool import Pool


def _pool(
    id_: str, size: int, offset: int | None = None, placed_at: int | None = None
) -> Pool:
    """One-allocation pool; `placed_at` places the allocation, `offset` the pool."""
    alloc = Allocation(id=f"{id_}.a", size=size, start=0, end=5, offset=placed_at)
    return Pool(id=id_, allocations=(alloc,), offset=offset)


@pytest.mark.parametrize("id_", [301, "mem_ddr"])
def test_creation_defaults(id_: int | str) -> None:
    memory = Memory(id=id_, pools=(_pool("p", 100), _pool("q", 200)))
    assert memory.id == id_
    assert len(memory.pools) == 2
    assert memory.size is None


@pytest.mark.parametrize("size", [0, 1000, 10**15])
def test_creation_with_size(size: int) -> None:
    assert Memory(id=1, pools=(), size=size).size == size


@pytest.mark.parametrize(
    ("pools", "size", "error", "match"),
    [
        ((), -1, ValueError, "size must be non-negative"),
        ((_pool("p", 1), _pool("p", 2)), None, ValueError, "pool ids must be unique"),
        ([1], None, TypeError, "Expected Pool"),
    ],
)
def test_construction_errors(
    pools: object, size: int | None, error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        Memory(id=1, pools=pools, size=size)


def test_memory_coerces_a_list_of_pools_to_a_tuple() -> None:
    memory = Memory(id="m", pools=[Pool(id="p", allocations=())])
    assert isinstance(memory.pools, tuple)
    assert hash(memory)


@pytest.mark.parametrize(
    ("pools", "expected"),
    [
        ((), True),
        ((_pool("p", 10, offset=0, placed_at=0), _pool("q", 20, 10, 0)), True),
        ((_pool("p", 10, offset=0), _pool("q", 20, offset=10)), False),
        ((_pool("p", 10, offset=0, placed_at=0), _pool("q", 20, offset=10)), False),
        ((_pool("p", 10, placed_at=0),), False),
    ],
)
def test_is_allocated_needs_placed_allocations_and_bases(
    pools: tuple[Pool, ...], expected: bool
) -> None:
    assert Memory(id="m", pools=pools).is_allocated is expected


def test_with_pools_keeps_id_and_size() -> None:
    memory = Memory(id=301, pools=(_pool("p", 100),), size=1000)
    replaced = memory.with_pools(())
    assert (replaced.id, replaced.size, replaced.pools) == (301, 1000, ())
    assert len(memory.pools) == 1


@pytest.mark.parametrize("field", ["id", "pools", "size"])
def test_fields_are_read_only(field: str) -> None:
    memory = Memory(id=301, pools=(), size=1000)
    with pytest.raises(AttributeError):
        setattr(memory, field, 7)


def test_extent_requires_placed_pools() -> None:
    memory = Memory(id="m", pools=(_pool("p", 10, placed_at=0),))
    with pytest.raises(ValueError, match="unplaced"):
        _ = memory.extent


@pytest.mark.parametrize(
    ("pools", "offsets"),
    [
        ((_pool("p1", 100), _pool("p2", 40)), [0, 100]),
        ((_pool("p1", 100, offset=500), _pool("p2", 40)), [500, 0]),
        ((_pool("pin", 10, 50), _pool("small", 20), _pool("large", 90)), [50, 0, 60]),
        ((_pool("p1", 100, offset=0), _pool("p2", 40, offset=100)), [0, 100]),
        ((Pool(id="empty", allocations=()), _pool("used", 10)), [0, 0]),
        ((_pool("p1", 100, 0), Pool(id="e", allocations=(), offset=50)), [0, 50]),
    ],
)
def test_allocate_places_pool_bases(
    pools: tuple[Pool, ...], offsets: list[int]
) -> None:
    memory = Memory(id="m", pools=pools, size=1000).allocate(NaiveAllocator())
    assert memory.is_allocated is True
    assert (memory.id, memory.size) == ("m", 1000)
    assert [pool.offset for pool in memory.pools] == offsets


def test_allocate_extent_covers_the_stacked_pools() -> None:
    memory = Memory(id="m", pools=(_pool("p1", 100), _pool("p2", 40)))
    assert memory.allocate(NaiveAllocator()).extent == 140


def test_allocate_rejects_overlapping_pinned_pool_bases() -> None:
    memory = Memory(id="m", pools=(_pool("p1", 100, 0), _pool("p2", 40, 50)))
    with pytest.raises(ValueError, match="pinned pools 'p1' and 'p2' already overlap"):
        memory.allocate(NaiveAllocator())


def test_allocate_accepts_an_empty_pool_pinned_inside_another() -> None:
    pools = (_pool("full", 16, offset=0), Pool(id="empty", allocations=(), offset=8))
    memory = Memory(id="m", pools=pools).allocate(NaiveAllocator())
    assert [pool.offset for pool in memory.pools] == [0, 8]
