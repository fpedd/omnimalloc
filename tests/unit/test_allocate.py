#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc import allocate
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.primitives import Allocation, AllocationKind, Memory, Pool, System

FIRST = Allocation(id="a", size=100, start=0, end=5, kind=AllocationKind.WORKSPACE)
SECOND = Allocation(id="b", size=150, start=5, end=10, kind=AllocationKind.CONSTANT)
THIRD = Allocation(id="c", size=50, start=0, end=5)
POOL = Pool(id="p", allocations=(FIRST, SECOND))


@pytest.mark.parametrize(
    "entity",
    [
        POOL,
        Pool(id="empty", allocations=()),
        Memory(id="m", pools=(POOL, Pool(id="q", allocations=(THIRD,)))),
        System(id="s", memories=(Memory(id="m", pools=(POOL,)),)),
    ],
)
def test_allocate_returns_the_entity_placed(entity: Pool | Memory | System) -> None:
    placed = allocate(entity, "naive", validate=True)
    assert type(placed) is type(entity)
    assert placed.is_allocated


@pytest.mark.parametrize("allocations", [(FIRST, SECOND), [FIRST, SECOND], ()])
def test_allocate_returns_raw_allocations_as_a_placed_tuple(
    allocations: list[Allocation] | tuple[Allocation, ...],
) -> None:
    placed = allocate(allocations, "naive", validate=True)
    assert type(placed) is tuple
    assert all(a.is_allocated for a in placed)


def test_allocate_keeps_everything_but_the_offsets() -> None:
    placed = allocate(Pool(id="p", allocations=(FIRST, SECOND), offset=50), "naive")
    assert placed.offset == 50
    assert placed.allocations == (FIRST.with_offset(0), SECOND.with_offset(100))
    assert placed.size == 250


def test_allocate_stacks_the_pools_of_a_memory() -> None:
    memory = Memory(
        id="m",
        pools=(Pool(id=1, allocations=(FIRST,)), Pool(id=2, allocations=(SECOND,))),
        size=1000,
    )
    assert allocate(memory, NaiveAllocator()).extent == 250


def test_allocate_leaves_its_input_alone() -> None:
    allocate((FIRST,), "naive")
    assert FIRST.offset is None


@pytest.mark.parametrize(
    ("entity", "error", "match"),
    [
        ((FIRST, FIRST), ValueError, "allocation ids must be unique"),
        ((1, 2, 3), TypeError, "Expected Allocation"),
        ("naive", TypeError, "Unsupported entity type"),
    ],
)
def test_allocate_rejects(entity: object, error: type[Exception], match: str) -> None:
    with pytest.raises(error, match=match):
        allocate(entity, NaiveAllocator())
