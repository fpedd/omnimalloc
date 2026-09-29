#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.naive import NaiveAllocator
from omnimalloc.allocators.omni import OmniAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.validate import validate_allocation


def _alloc(
    i: int, size: int, start: object, end: object, offset: int | None = None
) -> Allocation:
    return Allocation(id=i, size=size, start=start, end=end, offset=offset)


@pytest.mark.parametrize(
    ("allocator", "allocations", "offsets"),
    [
        pytest.param(
            OmniAllocator(),
            [_alloc(0, 10, 0, 5, offset=0), _alloc(1, 10, 2, 7)],
            [0, 10],
            id="blocks_a_conflicting_address",
        ),
        pytest.param(
            OmniAllocator(),
            [_alloc(0, 10, 0, 5, offset=0), _alloc(1, 10, 5, 9)],
            [0, 0],
            id="shares_its_address_after_its_lifetime",
        ),
        pytest.param(
            OmniAllocator(),
            [_alloc(0, 10, (0, 0), (4, 1), offset=64), _alloc(1, 10, (0, 0), (1, 4))],
            [64, 0],
            id="holds_under_vector_clocks",
        ),
        pytest.param(
            OmniAllocator(),
            [_alloc(0, 32, 0, 4, offset=64), *(_alloc(i, 32, 0, 4) for i in (1, 2, 3))],
            [64, 0, 32, 96],
            id="free_allocations_pack_around_it",
        ),
        pytest.param(
            OmniAllocator(),
            [_alloc(i, 10, 0, 5, offset=10 * i) for i in range(5)],
            [0, 10, 20, 30, 40],
            id="fully_pinned_input_stays_put",
        ),
        pytest.param(
            NaiveAllocator(),
            [_alloc(0, 10, 0, 5, offset=50), _alloc(1, 20, 0, 5), _alloc(2, 90, 0, 5)],
            [50, 0, 60],
            id="naive_fills_the_gap_below_it",
        ),
    ],
)
def test_a_pin(
    allocator: OmniAllocator | NaiveAllocator,
    allocations: list[Allocation],
    offsets: list[int],
) -> None:
    placed = allocator.allocate(tuple(allocations))
    assert [a.offset for a in placed] == offsets


def test_colliding_pins_are_rejected() -> None:
    allocations = (
        Allocation(id="a", size=10, start=0, end=5, offset=0),
        Allocation(id="b", size=10, start=1, end=6, offset=5),
    )
    with pytest.raises(ValueError, match="'a' and 'b' already collide"):
        OmniAllocator().allocate(allocations)


def test_many_allocations_place_validly_around_a_pin() -> None:
    allocations = (
        Allocation(id="pinned", size=64, start=0, end=10, offset=1024),
        *(Allocation(id=i, size=32, start=i, end=i + 4) for i in range(20)),
    )
    validate_allocation(OmniAllocator().allocate(allocations))


def test_staged_allocation_extends_an_earlier_placement() -> None:
    first = OmniAllocator().allocate(
        tuple(Allocation(id=i, size=16, start=i, end=i + 3) for i in range(8))
    )
    extra = tuple(Allocation(id=100 + i, size=24, start=i, end=i + 5) for i in range(4))
    second = OmniAllocator().allocate((*first, *extra))
    assert second[:8] == first
    validate_allocation(second)
